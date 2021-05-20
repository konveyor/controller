package model

import (
	"database/sql"
	"errors"
	"github.com/go-logr/logr"
	liberr "github.com/konveyor/controller/pkg/error"
	fb "github.com/konveyor/controller/pkg/filebacked"
	"os"
	"sync"
	"time"
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
	// Execute SQL.
	Execute(sql string) (sql.Result, error)
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
	dbMutex sync.RWMutex
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
func (r *Client) Open(purge bool) (err error) {
	if purge {
		_ = os.Remove(r.path)
	}
	r.db, err = sql.Open("sqlite3", r.path)
	if err != nil {
		r.log.V(3).Error(err, "open, failed.")
		panic(err)
	}
	defer func() {
		if err != nil {
			_ = r.db.Close()
			_ = os.Remove(r.path)
			r.db = nil
		}
	}()
	err = r.build()
	if err != nil {
		panic(err)
	}

	r.log.V(3).Info("DB opened.")

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
// Execute SQL.
func (r *Client) Execute(sql string) (sql.Result, error) {
	r.dbMutex.Lock()
	defer r.dbMutex.Unlock()
	return r.db.Exec(sql)
}

//
// Get the model.
func (r *Client) Get(model Model) (err error) {
	mark := time.Now()
	err = Table{r.db}.Get(model)
	if err == nil {
		r.log.V(4).Info(
			"get succeeded.",
			"model",
			Describe(model),
			"duration",
			time.Since(mark))
	}

	return
}

//
// List models.
// The `list` must be: *[]Model.
func (r *Client) List(list interface{}, options ListOptions) (err error) {
	mark := time.Now()
	r.dbMutex.RLock()
	defer r.dbMutex.RUnlock()
	err = Table{r.db}.List(list, options)
	if err == nil {
		r.log.V(4).Info(
			"list succeeded.",
			"options",
			"duration",
			time.Since(mark))
	}

	return
}

//
// List models.
func (r *Client) Iter(model interface{}, options ListOptions) (itr fb.Iterator, err error) {
	mark := time.Now()
	r.dbMutex.RLock()
	defer r.dbMutex.RUnlock()
	itr, err = Table{r.db}.Iter(model, options)
	if err == nil {
		r.log.V(4).Info(
			"iter succeeded",
			"options",
			options,
			"duration",
			time.Since(mark))
	}

	return
}

//
// Count models.
func (r *Client) Count(model Model, predicate Predicate) (n int64, err error) {
	mark := time.Now()
	n, err = Table{r.db}.Count(model, predicate)
	if err == nil {
		r.log.V(4).Info(
			"count succeeded.",
			"predicate",
			predicate,
			"duration",
			time.Since(mark))
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
	mark := time.Now()
	r.dbMutex.Lock()
	real, err := r.db.Begin()
	if err != nil {
		return nil, liberr.Wrap(
			err,
			"db",
			r.path)
	}
	tx := &Tx{
		labeler: r.labeler,
		dbMutex: &r.dbMutex,
		journal: &r.journal,
		started: time.Now(),
		log:     r.log,
		real:    real,
	}

	r.log.V(4).Info("tx begin.", "duration", time.Since(mark))

	return tx, nil
}

//
// Insert the model.
func (r *Client) Insert(model Model) error {
	mark := time.Now()
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
		Describe(model),
		"duration",
		time.Since(mark))

	return nil
}

//
// Update the model.
func (r *Client) Update(model Model) error {
	mark := time.Now()
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
		Describe(model),
		"duration",
		time.Since(mark))

	return nil
}

//
// Delete the model.
func (r *Client) Delete(model Model) error {
	mark := time.Now()
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
		Describe(model),
		"duration",
		time.Since(mark))

	return nil
}

//
// Watch model events.
func (r *Client) Watch(model Model, handler EventHandler) (w *Watch, err error) {
	r.dbMutex.RLock()
	defer r.dbMutex.RUnlock()
	mark := time.Now()
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
	options := handler.Options()
	var snapshot fb.Iterator
	if options.Snapshot {
		snapshot, err = Table{r.db}.Iter(model, ListOptions{Detail: 1})
		if err != nil {
			return
		}
	} else {
		snapshot = &fb.EmptyIterator{}
	}

	w.Start(snapshot)

	r.log.V(4).Info(
		"watch started.",
		"model",
		Describe(model),
		"options",
		options,
		"duration",
		time.Since(mark))

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
// Build schema.
func (r *Client) build() error {
	statements := []string{
		Pragma,
	}
	r.models = append(r.models, &Label{})
	for _, m := range r.models {
		ddl, err := Table{}.DDL(m)
		if err != nil {
			return err
		}
		statements = append(
			statements,
			ddl...)
	}
	for _, ddl := range statements {
		_, err := r.db.Exec(ddl)
		if err != nil {
			return liberr.Wrap(
				err,
				"DDL failed.",
				"ddl",
				ddl)
		} else {
			r.log.V(4).Info(
				"DDL succeeded.",
				"ddl",
				ddl)
		}
	}

	return nil
}

//
// Database transaction.
type Tx struct {
	labeler Labeler
	// Associated client.
	dbMutex *sync.RWMutex
	// Journal
	journal *Journal
	// Logger.
	log logr.Logger
	// Reference to real sql.Tx.
	real *sql.Tx
	// Mark.
	started time.Time
	// Ended
	ended bool
}

//
// Get the model.
func (r *Tx) Get(model Model) (err error) {
	err = Table{r.real}.Get(model)
	if err == nil {
		r.log.V(4).Info(
			"tx: get succeeded.",
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
			"tx: list succeeded.",
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
			"tx: iter succeeded",
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
			"tx: count succeeded.",
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
		"tx: model inserted.",
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
		"tx: model updated.",
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
		"tx: model deleted.",
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
	mark := time.Now()
	err = r.real.Commit()
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	r.journal.Committed()

	r.log.V(4).Info(
		"tx: committed.",
		"lifespan",
		time.Since(r.started),
		"duration",
		time.Since(mark))

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

	r.log.V(4).Info("tx: ended.")

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
