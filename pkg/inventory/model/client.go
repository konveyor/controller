package model

import (
	"database/sql"
	"errors"
	"github.com/go-logr/logr"
	liberr "github.com/konveyor/controller/pkg/error"
	fb "github.com/konveyor/controller/pkg/filebacked"
	"github.com/konveyor/controller/pkg/logging"
	"os"
	"sync"
)

const (
	Pragma = "PRAGMA foreign_keys = ON"
)

//
// Database client.
type DB interface {
	// Open and build the schema.
	Open(bool) error
	// Close.
	Close(bool) error
	// Get the specified model.
	Get(Model) error
	// List models based on the type of slice.
	List(interface{}, ListOptions) error
	// List models.
	Iter(interface{}, ListOptions) (fb.Iterator, error)
	// Count based on the specified model.
	Count(Model, Predicate) (int64, error)
	// Begin a transaction.
	Begin() (*Tx, error)
	// Insert a model.
	Insert(Model) error
	// Update a model.
	Update(Model) error
	// Delete a model.
	Delete(Model) error
	// Watch a model collection.
	Watch(Model, EventHandler) (*Watch, error)
	// End a watch.
	EndWatch(watch *Watch)
}

//
// Database client.
type Client struct {
	labeler Labeler
	// The sqlite3 database will not support
	// concurrent write operations.
	dbMutex sync.Mutex
	// file path.
	path string
	// Model
	models []interface{}
	// Database connection.
	db *sql.DB
	// Journal
	journal Journal
	// Logger
	log logr.Logger
}

//
// Create the database.
// Build the schema to support the specified models.
// Optionally `purge` (delete) the DB first.
func (r *Client) Open(purge bool) error {
	if purge {
		_ = os.Remove(r.path)
	}
	db, err := sql.Open("sqlite3", r.path)
	if err != nil {
		r.log.V(3).Error(err, "open, failed.")
		panic(err)
	}
	statements := []string{Pragma}
	r.models = append(r.models, &Label{})
	for _, m := range r.models {
		ddl, err := Table{}.DDL(m)
		if err != nil {
			panic(err)
		}
		statements = append(statements, ddl...)
	}
	for _, ddl := range statements {
		_, err = db.Exec(ddl)
		if err != nil {
			_ = db.Close()
			return liberr.Wrap(
				err,
				"DDL failed.",
				"ddl",
				ddl)
		}
		r.log.V(4).Info(
			"DDL executed.",
			"ddl",
			ddl)
	}

	r.log.V(3).Info("DB opened.")

	r.db = db

	return nil
}

//
// Close the database and associated journal.
// Optionally purge (delete) the DB.
func (r *Client) Close(purge bool) error {
	r.dbMutex.Lock()
	defer r.dbMutex.Unlock()
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	if err != nil {
		return liberr.Wrap(
			err,
			"DB close failed.",
			"path",
			r.path)
	}
	r.db = nil
	if purge {
		_ = os.Remove(r.path)
		r.log.V(3).Info("DB deleted.")
	}
	err = r.journal.Close()
	if err != nil {
		return err
	}

	r.log.V(3).Info("DB closed.")

	return nil
}

//
// Get the model.
func (r *Client) Get(model Model) (err error) {
	err = Table{r.db}.Get(model)
	if err == nil {
		r.log.V(4).Info(
			"get succeeded.",
			"model",
			Describe(model))
	}

	return
}

//
// List models.
// The `list` must be: *[]Model.
func (r *Client) List(list interface{}, options ListOptions) (err error) {
	err = Table{r.db}.List(list, options)
	if err == nil {
		r.log.V(4).Info(
			"list succeeded.",
			"options",
			options)
	}

	return
}

//
// List models.
func (r *Client) Iter(model interface{}, options ListOptions) (itr fb.Iterator, err error) {
	itr, err = Table{r.db}.Iter(model, options)
	if err == nil {
		r.log.V(4).Info(
			"iter succeeded",
			"options",
			options)
	}

	return
}

//
// Count models.
func (r *Client) Count(model Model, predicate Predicate) (n int64, err error) {
	n, err = Table{r.db}.Count(model, predicate)
	if err == nil {
		r.log.V(4).Info(
			"count succeeded.",
			"predicate",
			predicate)
	}
	return
}

//
// Begin a transaction.
// Example:
//   tx, _ := client.Begin()
//   defer tx.End()
//   client.Insert(model)
//   client.Insert(model)
//   tx.Commit()
func (r *Client) Begin() (*Tx, error) {
	r.dbMutex.Lock()
	real, err := r.db.Begin()
	if err != nil {
		return nil, liberr.Wrap(
			err,
			"db",
			r.path)
	}
	log := logging.WithName("model|db|tx").WithValues(
		"db",
		r.path,
		"tx",
		real)
	tx := &Tx{
		dbMutex: &r.dbMutex,
		journal: &r.journal,
		log:     log,
		real:    real,
	}

	r.log.V(4).Info("tx begin.")

	return tx, nil
}

//
// Insert the model.
func (r *Client) Insert(model Model) error {
	r.dbMutex.Lock()
	defer r.dbMutex.Unlock()
	table := Table{r.db}
	err := table.Insert(model)
	if err != nil {
		return err
	}
	err = r.labeler.Insert(table, model)
	if err != nil {
		return err
	}
	err = r.journal.Created(model, true)
	if err != nil {
		return err
	}

	r.log.V(4).Info(
		"model inserted.",
		"model",
		Describe(model))

	return nil
}

//
// Update the model.
func (r *Client) Update(model Model) error {
	r.dbMutex.Lock()
	defer r.dbMutex.Unlock()
	table := Table{r.db}
	current := model
	if r.journal.hasWatch(model) {
		current = Clone(model)
		err := table.Get(current)
		if err != nil {
			return err
		}
	}
	err := table.Update(model)
	if err != nil {
		return err
	}
	err = r.labeler.Replace(table, model)
	if err != nil {
		return err
	}
	err = r.journal.Updated(current, model, true)
	if err != nil {
		return err
	}

	r.log.V(4).Info(
		"model updated.",
		"model",
		Describe(model))

	return nil
}

//
// Delete the model.
func (r *Client) Delete(model Model) error {
	r.dbMutex.Lock()
	defer r.dbMutex.Unlock()
	table := Table{r.db}
	if r.journal.hasWatch(model) {
		err := table.Get(model)
		if err != nil {
			if errors.As(err, &NotFound) {
				return nil
			}
			return err
		}
	}
	err := table.Delete(model)
	if err != nil {
		return err
	}
	err = r.labeler.Delete(table, model)
	if err != nil {
		return err
	}
	err = r.journal.Deleted(model, true)
	if err != nil {
		return err
	}

	r.log.V(4).Info(
		"model deleted.",
		"model",
		Describe(model))

	return nil
}

//
// Watch model events.
func (r *Client) Watch(model Model, handler EventHandler) (w *Watch, err error) {
	w, err = r.journal.Watch(model, handler)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			w.End()
			w = nil
		}
	}()
	itr, err := Table{r.db}.Iter(
		model,
		ListOptions{Detail: 1})
	if err != nil {
		return nil, err
	}
	list := fb.NewList()
	for {
		model, hasNext, mErr := itr.Next()
		if mErr != nil {
			err = mErr
			return
		}
		if !hasNext {
			break
		}
		// Event.
		event := &Event{
			Action: Created,
		}
		mErr = list.Append(event)
		if mErr != nil {
			err = mErr
			return
		}
		// Model.
		mErr = list.Append(model)
		if mErr != nil {
			err = mErr
			return
		}
	}
	w.notify(list.Iter())
	w.Start()

	r.log.V(4).Info(
		"watch started.",
		"model",
		Describe(model),
		"count",
		itr.Len())

	return
}

//
// End watch.
func (r *Client) EndWatch(watch *Watch) {
	r.journal.End(watch)
	r.log.V(4).Info(
		"watch ended.",
		"model",
		watch.Model.String())
}

//
// Database transaction.
type Tx struct {
	labeler Labeler
	// Associated client.
	dbMutex *sync.Mutex
	// Journal
	journal *Journal
	// Logger.
	log logr.Logger
	// Reference to real sql.Tx.
	real *sql.Tx
	// Ended
	ended bool
}

//
// Get the model.
func (r *Tx) Get(model Model) (err error) {
	err = Table{r.real}.Get(model)
	if err == nil {
		r.log.V(4).Info(
			"get succeeded.",
			"model",
			Describe(model))
	}

	return
}

//
// List models.
// The `list` must be: *[]Model.
func (r *Tx) List(list interface{}, options ListOptions) (err error) {
	err = Table{r.real}.List(list, options)
	if err == nil {
		r.log.V(4).Info(
			"list succeeded.",
			"options",
			options)
	}

	return
}

//
// List models.
func (r *Tx) Iter(model interface{}, options ListOptions) (itr fb.Iterator, err error) {
	itr, err = Table{r.real}.Iter(model, options)
	if err == nil {
		r.log.V(4).Info(
			"iter succeeded",
			"options",
			options)
	}

	return
}

//
// Count models.
func (r *Tx) Count(model Model, predicate Predicate) (n int64, err error) {
	n, err = Table{r.real}.Count(model, predicate)
	if err == nil {
		r.log.V(4).Info(
			"count succeeded.",
			"predicate",
			predicate)
	}

	return
}

//
// Insert the model.
func (r *Tx) Insert(model Model) error {
	table := Table{r.real}
	err := table.Insert(model)
	if err != nil {
		return err
	}
	err = r.labeler.Insert(table, model)
	if err != nil {
		return err
	}
	err = r.journal.Created(model, false)
	if err != nil {
		return err
	}

	r.log.V(4).Info(
		"model inserted.",
		"model",
		Describe(model))

	return nil
}

//
// Update the model.
func (r *Tx) Update(model Model) error {
	table := Table{r.real}
	current := model
	if r.journal.hasWatch(model) {
		current = Clone(model)
		err := table.Get(current)
		if err != nil {
			if errors.As(err, &NotFound) {
				return nil
			}
			return err
		}
	}
	err := table.Update(model)
	if err != nil {
		return err
	}
	err = r.labeler.Replace(table, model)
	if err != nil {
		return err
	}
	err = r.journal.Updated(current, model, false)
	if err != nil {
		return err
	}

	r.log.V(4).Info(
		"model updated.",
		"model",
		Describe(model))

	return nil
}

//
// Delete the model.
func (r *Tx) Delete(model Model) error {
	table := Table{r.real}
	if r.journal.hasWatch(model) {
		err := table.Get(model)
		if err != nil {
			if errors.As(err, &NotFound) {
				return nil
			}
			return err
		}
	}
	err := table.Delete(model)
	if err != nil {
		return err
	}
	err = r.labeler.Delete(table, model)
	if err != nil {
		return err
	}
	err = r.journal.Deleted(model, false)
	if err != nil {
		return err
	}

	r.log.V(4).Info(
		"model deleted.",
		"model",
		Describe(model))

	return nil
}

//
// Commit a transaction.
// Staged changes are committed in the DB.
// This will end the transaction.
func (r *Tx) Commit() (err error) {
	if r.ended {
		return
	}
	defer func() {
		r.dbMutex.Unlock()
		r.ended = true
	}()
	err = r.real.Commit()
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	r.journal.Committed()

	r.log.V(4).Info("tx committed.")

	return
}

//
// End a transaction.
// Staged changes are discarded.
// See: Commit().
func (r *Tx) End() (err error) {
	if r.ended {
		return
	}
	defer func() {
		r.dbMutex.Unlock()
		r.ended = true
	}()
	err = r.real.Rollback()
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	r.journal.Reset()

	r.log.V(4).Info("tx ended.")

	return
}

//
// Labeler.
type Labeler struct {
	// Logger.
	log logr.Logger
}

//
// Insert labels for the model into the DB.
func (r *Labeler) Insert(table Table, model Model) error {
	kind := table.Name(model)
	for l, v := range model.Labels() {
		label := &Label{
			Parent: model.Pk(),
			Kind:   kind,
			Name:   l,
			Value:  v,
		}
		err := table.Insert(label)
		if err != nil {
			return err
		}
		r.log.V(2).Info(
			"label inserted.",
			"model",
			Describe(model),
			"kind",
			kind,
			"label",
			l,
			"value",
			v)
	}

	return nil
}

//
// Delete labels for a model in the DB.
func (r *Labeler) Delete(table Table, model Model) error {
	list := []Label{}
	err := table.List(
		&list,
		ListOptions{
			Predicate: And(
				Eq("Kind", table.Name(model)),
				Eq("Parent", model.Pk())),
		})
	if err != nil {
		return err
	}
	for _, label := range list {
		err := table.Delete(&label)
		if err != nil {
			return err
		}
		r.log.V(2).Info(
			"label inserted.",
			"model",
			Describe(model),
			"kind",
			label.Kind,
			"label",
			label.Name,
			"value",
			label.Value)
	}

	return nil
}

//
// Replace labels.
func (r *Labeler) Replace(table Table, model Model) error {
	err := r.Delete(table, model)
	if err != nil {
		return err
	}
	err = r.Insert(table, model)
	if err != nil {
		return err
	}

	return nil
}
