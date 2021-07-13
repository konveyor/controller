package model

import (
	"bytes"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	liberr "github.com/konveyor/controller/pkg/error"
	fb "github.com/konveyor/controller/pkg/filebacked"
	"github.com/mattn/go-sqlite3"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

const (
	// SQL tag.
	Tag = "sql"
	// Max detail level.
	MaxDetail = 10
)

//
// DDL templates.
var TableDDL = `
CREATE TABLE IF NOT EXISTS {{.Table}} (
{{ range $i,$f := .Fields -}}
{{ if $i }},{{ end -}}
{{ $f.DDL }}
{{ end -}}
{{ range $i,$c := .Constraints -}}
,{{ $c }}
{{ end -}}
);
`

var IndexDDL = `
CREATE INDEX IF NOT EXISTS {{.Index}}Index
ON {{.Table}}
(
{{ range $i,$f := .Fields -}}
{{ if $i }},{{ end -}}
{{ $f.Name }}
{{ end -}}
);
`

//
// SQL templates.
var InsertSQL = `
INSERT INTO {{.Table}} (
{{ range $i,$f := .Fields -}}
{{ if $i}},{{ end -}}
{{ $f.Name }}
{{ end -}}
)
VALUES (
{{ range $i,$f := .Fields -}}
{{ if $i }},{{ end -}}
{{ $f.Param }}
{{ end -}}
);
`

var UpdateSQL = `
UPDATE {{.Table}}
SET
{{ range $i,$f := .Fields -}}
{{ if $i }},{{ end -}}
{{ $f.Name }} = {{ $f.Param }}
{{ end -}}
WHERE
{{ .Pk.Name }} = {{ .Pk.Param }}
;
`

var DeleteSQL = `
DELETE FROM {{.Table}}
WHERE
{{ .Pk.Name }} = {{ .Pk.Param }}
;
`

var GetSQL = `
SELECT
{{ range $i,$f := .Fields -}}
{{ if $i }},{{ end -}}
{{ $f.Name }}
{{ end -}}
FROM {{.Table}}
WHERE
{{ .Pk.Name }} = {{ .Pk.Param }}
;
`

var ListSQL = `
SELECT
{{ if .Count -}}
COUNT(*)
{{ else -}}
{{ range $i,$f := .Options.Fields -}}
{{ if $i }},{{ end -}}
{{ $f.Name }}
{{ end -}}
{{ end -}}
FROM {{.Table}}
{{ if or .Predicate -}}
WHERE
{{ end -}}
{{ if .Predicate -}}
{{ .Predicate.Expr }}
{{ end -}}
{{ if .Sort -}}
ORDER BY
{{ range $i,$n := .Sort -}}
{{ if $i }},{{ end }}{{ $n }}
{{ end -}}
{{ end -}}
{{ if .Page -}}
LIMIT {{.Page.Limit}} OFFSET {{.Page.Offset}}
{{ end -}}
;
`

//
// Errors
var (
	// Must have PK.
	MustHavePkErr = errors.New("must have PK field")
	// Parameter must be pointer error.
	MustBePtrErr = errors.New("must be pointer")
	// Must be slice pointer.
	MustBeSlicePtrErr = errors.New("must be slice pointer")
	// Parameter must be struct error.
	MustBeObjectErr = errors.New("must be object")
	// Field type error.
	FieldTypeErr = errors.New("field type must be (int, str, bool")
	// PK field type error.
	PkTypeErr = errors.New("pk field must be (int, str)")
	// Generated PK error.
	GenPkTypeErr = errors.New("PK field must be `str` when generated")
	// Invalid field referenced in predicate.
	PredicateRefErr = errors.New("predicate referenced unknown field")
	// Invalid predicate for type of field.
	PredicateTypeErr = errors.New("predicate type not valid for field")
	// Invalid predicate value.
	PredicateValueErr = errors.New("predicate value not valid")
)

//
// Represents a table in the DB.
// Using reflect, the model is inspected to determine the
// table name and columns. The column definition is specified
// using field tags:
//   pk - Primary key.
//   key - Natural key.
//   fk:<table>(field) - Foreign key.
//   unique(<group>) - Unique constraint collated by <group>.
//   const - Not updated.
type Table struct {
	// Database connection.
	DB DBTX
}

//
// Get the table name for the model.
func (t Table) Name(model interface{}) string {
	mt := reflect.TypeOf(model)
	if mt.Kind() == reflect.Ptr {
		mt = mt.Elem()
	}

	return mt.Name()
}

//
// Validate the model.
func (t Table) Validate(fields []*Field) error {
	for _, f := range fields {
		err := f.Validate()
		if err != nil {
			return err
		}
	}
	pk := t.PkField(fields)
	if pk == nil {
		return liberr.Wrap(MustHavePkErr)
	}

	return nil
}

//
// Get table and index create DDL.
func (t Table) DDL(model interface{}) (list []string, err error) {
	list = []string{}
	fields, err := t.Fields(model)
	if err != nil {
		return
	}
	err = t.Validate(fields)
	if err != nil {
		return
	}
	ddl, err := t.TableDDL(model, fields)
	if err != nil {
		return
	}
	for _, stmt := range ddl {
		list = append(list, stmt)
	}
	ddl, err = t.KeyIndexDDL(model, fields)
	if err != nil {
		return
	}
	for _, stmt := range ddl {
		list = append(list, stmt)
	}
	ddl, err = t.IndexDDL(model, fields)
	if err != nil {
		return
	}
	for _, stmt := range ddl {
		list = append(list, stmt)
	}

	return
}

//
// Build table DDL.
func (t Table) TableDDL(model interface{}, fields []*Field) (list []string, err error) {
	tpl := template.New("")
	tpl, err = tpl.Parse(TableDDL)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	constraints := t.Constraints(fields)
	bfr := &bytes.Buffer{}
	err = tpl.Execute(
		bfr,
		TmplData{
			Table:       t.Name(model),
			Fields:      t.RealFields(fields),
			Constraints: constraints,
		})
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	list = append(list, bfr.String())
	return
}

//
// Build natural key index DDL.
func (t Table) KeyIndexDDL(model interface{}, fields []*Field) (list []string, err error) {
	tpl := template.New("")
	keyFields := t.KeyFields(fields)
	if len(keyFields) > 0 {
		tpl, err = tpl.Parse(IndexDDL)
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
		bfr := &bytes.Buffer{}
		err = tpl.Execute(
			bfr,
			TmplData{
				Table:  t.Name(model),
				Index:  t.Name(model),
				Fields: t.RealFields(keyFields),
			})
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
		list = append(list, bfr.String())
	}

	return
}

//
// Build non-unique index DDL.
func (t Table) IndexDDL(model interface{}, fields []*Field) (list []string, err error) {
	tpl := template.New("")
	index := map[string][]*Field{}
	for _, field := range fields {
		for _, group := range field.Index() {
			list, found := index[group]
			if found {
				index[group] = append(list, field)
			} else {
				index[group] = []*Field{field}
			}
		}
	}
	for group, idxFields := range index {
		tpl, err = tpl.Parse(IndexDDL)
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
		bfr := &bytes.Buffer{}
		err = tpl.Execute(
			bfr,
			TmplData{
				Table:  t.Name(model),
				Index:  t.Name(model) + group,
				Fields: t.RealFields(idxFields),
			})
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
		list = append(list, bfr.String())
	}

	return
}

//
// Insert the model in the DB.
// Expects the primary key (PK) to be set.
func (t Table) Insert(model interface{}) error {
	fields, err := t.Fields(model)
	if err != nil {
		return err
	}
	t.EnsurePk(fields)
	stmt, err := t.insertSQL(t.Name(model), fields)
	if err != nil {
		return err
	}
	params := t.Params(fields)
	r, err := t.DB.Exec(stmt, params...)
	if err != nil {
		if sql3Err, cast := err.(sqlite3.Error); cast {
			if sql3Err.Code == sqlite3.ErrConstraint {
				return t.Update(model)
			}
		}
		return liberr.Wrap(err, "sql", stmt, "params", params)
	}
	_, err = r.RowsAffected()
	if err != nil {
		return liberr.Wrap(err)
	}

	t.reflectIncremented(fields)

	log.V(5).Info(
		"table: model inserted.",
		"sql",
		stmt,
		"params",
		params)

	return nil
}

//
// Update the model in the DB.
// Expects the primary key (PK) or natural keys to be set.
func (t Table) Update(model interface{}) error {
	fields, err := t.Fields(model)
	if err != nil {
		return err
	}
	t.EnsurePk(fields)
	stmt, err := t.updateSQL(t.Name(model), fields)
	if err != nil {
		return err
	}
	params := t.Params(fields)
	r, err := t.DB.Exec(stmt, params...)
	if err != nil {
		return liberr.Wrap(err, "sql", stmt, "params", params)
	}
	nRows, err := r.RowsAffected()
	if err != nil {
		return liberr.Wrap(err)
	}
	if nRows == 0 {
		return liberr.Wrap(NotFound)
	}

	t.reflectIncremented(fields)

	log.V(5).Info(
		"table: model updated.",
		"sql",
		stmt,
		"params",
		params)

	return nil
}

//
// Delete the model in the DB.
// Expects the primary key (PK) or natural keys to be set.
func (t Table) Delete(model interface{}) error {
	fields, err := t.Fields(model)
	if err != nil {
		return err
	}
	t.EnsurePk(fields)
	stmt, err := t.deleteSQL(t.Name(model), fields)
	if err != nil {
		return err
	}
	params := t.Params(fields)
	r, err := t.DB.Exec(stmt, params...)
	if err != nil {
		return liberr.Wrap(err, "sql", stmt, "params", params)
	}
	nRows, err := r.RowsAffected()
	if err != nil {
		return liberr.Wrap(err)
	}
	if nRows == 0 {
		return nil
	}

	log.V(5).Info(
		"table: model deleted.",
		"sql",
		stmt,
		"params",
		params)

	return nil
}

//
// Get the model in the DB.
// Expects the primary key (PK) or natural keys to be set.
// Fetch the row and populate the fields in the model.
func (t Table) Get(model interface{}) error {
	fields, err := t.Fields(model)
	if err != nil {
		return err
	}
	t.EnsurePk(fields)
	stmt, err := t.getSQL(t.Name(model), fields)
	if err != nil {
		return err
	}
	params := t.Params(fields)
	row := t.DB.QueryRow(stmt, params...)
	err = t.scan(row, fields)
	if err != nil {
		err = liberr.Wrap(err, "sql", stmt, "params", params)
	}

	log.V(5).Info(
		"table: get succeeded.",
		"sql",
		stmt,
		"params",
		params)

	return err
}

//
// List the model in the DB.
// Qualified by the list options.
func (t Table) List(list interface{}, options ListOptions) error {
	var model interface{}
	lt := reflect.TypeOf(list)
	lv := reflect.ValueOf(list)
	switch lt.Kind() {
	case reflect.Ptr:
		lt = lt.Elem()
		lv = lv.Elem()
	default:
		return liberr.Wrap(MustBeSlicePtrErr)
	}
	switch lt.Kind() {
	case reflect.Slice:
		model = reflect.New(lt.Elem()).Interface()
	default:
		return liberr.Wrap(MustBeSlicePtrErr)
	}
	fields, err := t.Fields(model)
	if err != nil {
		return err
	}
	stmt, err := t.listSQL(t.Name(model), fields, &options)
	if err != nil {
		return err
	}
	params := options.Params()
	cursor, err := t.DB.Query(stmt, params...)
	if err != nil {
		return liberr.Wrap(err, "sql", stmt, "params", params)
	}
	defer func() {
		_ = cursor.Close()
	}()
	mList := reflect.MakeSlice(lt, 0, 0)
	for cursor.Next() {
		mt := reflect.TypeOf(model)
		mPtr := reflect.New(mt.Elem())
		mInt := mPtr.Interface()
		mFields, _ := t.Fields(mInt)
		options.fields = mFields
		err = t.scan(cursor, options.Fields())
		if err != nil {
			return liberr.Wrap(err)
		}
		mList = reflect.Append(mList, mPtr.Elem())
	}

	lv.Set(mList)

	log.V(5).Info(
		"table: list succeeded.",
		"sql",
		stmt,
		"params",
		params,
		"matched",
		lv.Len())

	return nil
}

//
// Find models in the DB.
// Qualified by the list options.
func (t Table) Find(model interface{}, options ListOptions) (itr fb.Iterator, err error) {
	fields, err := t.Fields(model)
	if err != nil {
		return
	}
	stmt, err := t.listSQL(t.Name(model), fields, &options)
	if err != nil {
		return
	}
	params := options.Params()
	cursor, err := t.DB.Query(stmt, params...)
	if err != nil {
		err = liberr.Wrap(err, "sql", stmt, "params", params)
		return
	}
	defer func() {
		_ = cursor.Close()
	}()
	list := fb.NewList()
	for cursor.Next() {
		mt := reflect.TypeOf(model)
		mPtr := reflect.New(mt.Elem())
		mInt := mPtr.Interface()
		mFields, _ := t.Fields(mInt)
		options.fields = mFields
		err = t.scan(cursor, options.Fields())
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
		list.Append(mPtr.Interface())
	}

	itr = list.Iter()

	log.V(5).Info(
		"table: find succeeded.",
		"sql",
		stmt,
		"params",
		params,
		"matched",
		itr.Len())

	return
}

//
// Count the models in the DB.
// Qualified by the model field values and list options.
// Expects natural keys to be set.
// Else, ALL models counted.
func (t Table) Count(model interface{}, predicate Predicate) (int64, error) {
	fields, err := t.Fields(model)
	if err != nil {
		return 0, err
	}
	options := ListOptions{Predicate: predicate}
	stmt, err := t.countSQL(t.Name(model), fields, &options)
	if err != nil {
		return 0, err
	}
	count := int64(0)
	params := options.Params()
	row := t.DB.QueryRow(stmt, params...)
	err = row.Scan(&count)
	if err != nil {
		return 0, liberr.Wrap(err, "sql", stmt, "params", params)
	}

	log.V(5).Info(
		"table: count succeeded.",
		"sql",
		stmt,
		"params",
		params)

	return count, nil
}

//
// Get the `Fields` for the model.
func (t Table) Fields(model interface{}) ([]*Field, error) {
	fields := []*Field{}
	mt := reflect.TypeOf(model)
	mv := reflect.ValueOf(model)
	if mt.Kind() == reflect.Ptr {
		mt = mt.Elem()
		mv = mv.Elem()
	} else {
		return nil, liberr.Wrap(MustBePtrErr)
	}
	if mv.Kind() != reflect.Struct {
		return nil, liberr.Wrap(MustBeObjectErr)
	}
	for i := 0; i < mt.NumField(); i++ {
		ft := mt.Field(i)
		fv := mv.Field(i)
		if !fv.CanSet() {
			continue
		}
		switch fv.Kind() {
		case reflect.Struct:
			sqlTag, found := ft.Tag.Lookup(Tag)
			if found {
				if sqlTag == "-" {
					break
				}
				fields = append(
					fields,
					&Field{
						Tag:   sqlTag,
						Name:  ft.Name,
						Value: &fv,
						Type:  &ft,
					})
			} else {
				nested, err := t.Fields(fv.Addr().Interface())
				if err != nil {
					return nil, nil
				}
				fields = append(fields, nested...)
			}
		case reflect.Slice,
			reflect.Map,
			reflect.String,
			reflect.Bool,
			reflect.Int,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64:
			sqlTag, _ := ft.Tag.Lookup(Tag)
			if sqlTag == "-" {
				continue
			}
			fields = append(
				fields,
				&Field{
					Tag:   sqlTag,
					Name:  ft.Name,
					Value: &fv,
					Type:  &ft,
				})
		}
	}

	return fields, nil
}

//
// Get the `Fields` referenced as param in SQL.
func (t Table) Params(fields []*Field) []interface{} {
	list := []interface{}{}
	for _, f := range fields {
		if f.isParam {
			p := sql.Named(f.Name, f.Pull())
			list = append(list, p)
		}
	}

	return list
}

//
// Ensure PK is generated as specified/needed.
func (t Table) EnsurePk(fields []*Field) {
	pk := t.PkField(fields)
	if pk == nil {
		return
	}
	withFields := pk.WithFields()
	if len(withFields) == 0 {
		return
	}
	switch pk.Value.Kind() {
	case reflect.String:
		if pk.Pull() != "" {
			return
		}
	default:
		return
	}
	h := sha1.New()
	for _, f := range fields {
		name := strings.ToLower(f.Name)
		if matched, _ := withFields[name]; !matched {
			continue
		}
		f.Pull()
		switch f.Value.Kind() {
		case reflect.String:
			h.Write([]byte(f.string))
		case reflect.Bool,
			reflect.Int,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64:
			bfr := new(bytes.Buffer)
			binary.Write(bfr, binary.BigEndian, f.int)
			h.Write(bfr.Bytes())
		}
	}
	pk.string = hex.EncodeToString(h.Sum(nil))
	pk.Push()
}

//
// Get the mutable `Fields` for the model.
func (t Table) MutableFields(fields []*Field) []*Field {
	list := []*Field{}
	for _, f := range fields {
		if f.Mutable() {
			list = append(list, f)
		}
	}

	return list
}

//
// Get the natural key `Fields` for the model.
func (t Table) KeyFields(fields []*Field) []*Field {
	list := []*Field{}
	for _, f := range fields {
		if f.Key() {
			list = append(list, f)
		}
	}

	return list
}

//
// Get the non-virtual `Fields` for the model.
func (t Table) RealFields(fields []*Field) []*Field {
	list := []*Field{}
	for _, f := range fields {
		if !f.Virtual() {
			list = append(list, f)
		}
	}

	return list
}

//
// Get the PK field.
func (t Table) PkField(fields []*Field) *Field {
	for _, f := range fields {
		if f.Pk() {
			return f
		}
	}

	return nil
}

//
// Get constraint DDL.
func (t Table) Constraints(fields []*Field) []string {
	constraints := []string{}
	unique := map[string][]string{}
	for _, field := range fields {
		for _, name := range field.Unique() {
			list, found := unique[name]
			if found {
				unique[name] = append(list, field.Name)
			} else {
				unique[name] = []string{field.Name}
			}
		}
	}
	for _, list := range unique {
		constraints = append(
			constraints,
			fmt.Sprintf(
				"UNIQUE (%s)",
				strings.Join(list, ",")))
	}
	for _, field := range fields {
		fk := field.Fk()
		if fk == nil {
			continue
		}
		constraints = append(constraints, fk.DDL(field))
	}

	return constraints
}

//
// Reflect auto-incremented fields.
// Field.int is incremented by Field.Push() called when the
// SQL statement is built. This needs to be propagated to the model.
func (t *Table) reflectIncremented(fields []*Field) {
	for _, f := range fields {
		if f.Incremented() {
			f.Value.SetInt(f.int)
		}
	}
}

//
// Build model insert SQL.
func (t Table) insertSQL(table string, fields []*Field) (string, error) {
	tpl := template.New("")
	tpl, err := tpl.Parse(InsertSQL)
	if err != nil {
		return "", liberr.Wrap(err)
	}
	bfr := &bytes.Buffer{}
	err = tpl.Execute(
		bfr,
		TmplData{
			Table:  table,
			Fields: t.RealFields(fields),
		})
	if err != nil {
		return "", liberr.Wrap(err)
	}

	return bfr.String(), nil
}

//
// Build model update SQL.
func (t Table) updateSQL(table string, fields []*Field) (string, error) {
	tpl := template.New("")
	tpl, err := tpl.Parse(UpdateSQL)
	if err != nil {
		return "", liberr.Wrap(err)
	}
	bfr := &bytes.Buffer{}
	err = tpl.Execute(
		bfr,
		TmplData{
			Table:  table,
			Fields: t.MutableFields(fields),
			Pk:     t.PkField(fields),
		})
	if err != nil {
		return "", liberr.Wrap(err)
	}

	return bfr.String(), nil
}

//
// Build model delete SQL.
func (t Table) deleteSQL(table string, fields []*Field) (string, error) {
	tpl := template.New("")
	tpl, err := tpl.Parse(DeleteSQL)
	if err != nil {
		return "", liberr.Wrap(err)
	}
	bfr := &bytes.Buffer{}
	err = tpl.Execute(
		bfr,
		TmplData{
			Table: table,
			Pk:    t.PkField(fields),
		})
	if err != nil {
		return "", liberr.Wrap(err)
	}

	return bfr.String(), nil
}

//
// Build model get SQL.
func (t Table) getSQL(table string, fields []*Field) (string, error) {
	tpl := template.New("")
	tpl, err := tpl.Parse(GetSQL)
	if err != nil {
		return "", liberr.Wrap(err)
	}
	bfr := &bytes.Buffer{}
	err = tpl.Execute(
		bfr,
		TmplData{
			Table:  table,
			Pk:     t.PkField(fields),
			Fields: fields,
		})
	if err != nil {
		return "", liberr.Wrap(err)
	}

	return bfr.String(), nil
}

//
// Build model list SQL.
func (t Table) listSQL(table string, fields []*Field, options *ListOptions) (string, error) {
	tpl := template.New("")
	tpl, err := tpl.Parse(ListSQL)
	if err != nil {
		return "", liberr.Wrap(err)
	}
	err = options.Build(table, fields)
	if err != nil {
		return "", err
	}
	bfr := &bytes.Buffer{}
	err = tpl.Execute(
		bfr,
		TmplData{
			Table:   table,
			Fields:  fields,
			Options: options,
			Pk:      t.PkField(fields),
		})
	if err != nil {
		return "", liberr.Wrap(err)
	}

	return bfr.String(), nil
}

//
// Build model count SQL.
func (t Table) countSQL(table string, fields []*Field, options *ListOptions) (string, error) {
	tpl := template.New("")
	tpl, err := tpl.Parse(ListSQL)
	if err != nil {
		return "", liberr.Wrap(err)
	}
	err = options.Build(table, fields)
	if err != nil {
		return "", err
	}
	bfr := &bytes.Buffer{}
	err = tpl.Execute(
		bfr,
		TmplData{
			Table:   table,
			Fields:  fields,
			Options: options,
			Count:   true,
			Pk:      t.PkField(fields),
		})
	if err != nil {
		return "", liberr.Wrap(err)
	}

	return bfr.String(), nil
}

//
// Scan the fetch row into the model.
// The model fields are updated.
func (t Table) scan(row Row, fields []*Field) error {
	list := []interface{}{}
	for _, f := range fields {
		f.Pull()
		list = append(list, f.Ptr())
	}
	err := row.Scan(list...)
	if err == nil {
		for _, f := range fields {
			f.Push()
		}
	}

	return liberr.Wrap(err)
}

//
// Regex used for `pk(fields)` tags.
var PkRegex = regexp.MustCompile(`(pk)((\()(.+)(\)))?`)

//
// Regex used for `unique(group)` tags.
var UniqueRegex = regexp.MustCompile(`(unique)(\()(.+)(\))`)

//
// Regex used for `index(group)` tags.
var IndexRegex = regexp.MustCompile(`(index)(\()(.+)(\))`)

//
// Regex used for `fk:<table>(field)` tags.
var FkRegex = regexp.MustCompile(`(fk):(.+)(\()(.+)(\))`)

//
// Model (struct) Field
type Field struct {
	// reflect.Type of the field.
	Type *reflect.StructField
	// reflect.Value of the field.
	Value *reflect.Value
	// Field name.
	Name string
	// SQL tag.
	Tag string
	// Staging (string) values.
	string string
	// Staging (int) values.
	int int64
	// Referenced as a parameter.
	isParam bool
}

//
// Validate.
func (f *Field) Validate() error {
	switch f.Value.Kind() {
	case reflect.String:
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		if len(f.WithFields()) > 0 {
			return liberr.Wrap(GenPkTypeErr)
		}
	default:
		if f.Pk() {
			return liberr.Wrap(PkTypeErr)
		}
	}

	return nil
}

//
// Pull from model.
// Populate the appropriate `staging` field using the
// model field value.
func (f *Field) Pull() interface{} {
	switch f.Value.Kind() {
	case reflect.Struct:
		object := f.Value.Interface()
		b, err := json.Marshal(&object)
		if err == nil {
			f.string = string(b)
		}
		return f.string
	case reflect.Slice:
		if !f.Value.IsNil() {
			object := f.Value.Interface()
			b, err := json.Marshal(&object)
			if err == nil {
				f.string = string(b)
			}
		} else {
			f.string = "[]"
		}
		return f.string
	case reflect.Map:
		if !f.Value.IsNil() {
			object := f.Value.Interface()
			b, err := json.Marshal(&object)
			if err == nil {
				f.string = string(b)
			}
		} else {
			f.string = "{}"
		}
		return f.string
	case reflect.String:
		f.string = f.Value.String()
		return f.string
	case reflect.Bool:
		b := f.Value.Bool()
		if b {
			f.int = 1
		}
		return f.int
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		f.int = f.Value.Int()
		if f.Incremented() {
			f.int++
		}
		return f.int
	}

	return nil
}

//
// Pointer used for Scan().
func (f *Field) Ptr() interface{} {
	switch f.Value.Kind() {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		return &f.int
	default:
		return &f.string
	}
}

//
// Push to the model.
// Set the model field value using the `staging` field.
func (f *Field) Push() {
	switch f.Value.Kind() {
	case reflect.Struct:
		if len(f.string) == 0 {
			break
		}
		tv := reflect.New(f.Value.Type())
		object := tv.Interface()
		err := json.Unmarshal([]byte(f.string), &object)
		if err == nil {
			tv = reflect.ValueOf(object)
			f.Value.Set(tv.Elem())
		}
	case reflect.Slice,
		reflect.Map:
		if len(f.string) == 0 {
			break
		}
		tv := reflect.New(f.Value.Type())
		object := tv.Interface()
		err := json.Unmarshal([]byte(f.string), object)
		if err == nil {
			tv = reflect.ValueOf(object)
			tv = reflect.Indirect(tv)
			f.Value.Set(tv)
		}
	case reflect.String:
		f.Value.SetString(f.string)
	case reflect.Bool:
		b := false
		if f.int != 0 {
			b = true
		}
		f.Value.SetBool(b)
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		f.Value.SetInt(f.int)
	}
}

//
// Column DDL.
func (f *Field) DDL() string {
	part := []string{
		f.Name, // name
		"",     // type
		"",     // constraint
	}
	switch f.Value.Kind() {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		part[1] = "INTEGER"
	default:
		part[1] = "TEXT"
	}
	if f.Pk() {
		part[2] = "PRIMARY KEY"
	} else {
		part[2] = "NOT NULL"
	}

	return strings.Join(part, " ")
}

//
// Get as SQL param.
func (f *Field) Param() string {
	f.isParam = true
	return ":" + f.Name
}

//
// Get whether field is the primary key.
func (f *Field) Pk() (matched bool) {
	for _, opt := range strings.Split(f.Tag, ",") {
		m := PkRegex.FindStringSubmatch(opt)
		if m != nil {
			matched = true
			break
		}
	}
	return
}

//
// Fields used to generate the primary key.
// Map of lower-cased field names. May be empty
// when generation is not enabled.
func (f *Field) WithFields() (withFields map[string]bool) {
	withFields = map[string]bool{}
	for _, opt := range strings.Split(f.Tag, ",") {
		opt = strings.TrimSpace(opt)
		m := PkRegex.FindStringSubmatch(opt)
		if m != nil && len(m) == 6 {
			for _, name := range strings.Split(m[4], ";") {
				name = strings.TrimSpace(name)
				if len(name) > 0 {
					name = strings.ToLower(name)
					withFields[name] = true
				}
			}
		}
		break
	}

	return
}

//
// Get whether field is mutable.
// Only mutable fields will be updated.
func (f *Field) Mutable() bool {
	if f.Pk() || f.Key() || f.Virtual() {
		return false
	}

	return !f.hasOpt("const")
}

//
// Get whether field is a natural key.
func (f *Field) Key() bool {
	return f.hasOpt("key")
}

//
// Get whether field is virtual.
// A `virtual` field is read-only and managed
// internally in the DB.
func (f *Field) Virtual() bool {
	return f.hasOpt("virtual")
}

//
// Get whether the field is unique.
func (f *Field) Unique() []string {
	list := []string{}
	for _, opt := range strings.Split(f.Tag, ",") {
		opt = strings.TrimSpace(opt)
		m := UniqueRegex.FindStringSubmatch(opt)
		if m != nil && len(m) == 5 {
			list = append(list, m[3])
		}
	}

	return list
}

//
// Get whether the field has non-unique index.
func (f *Field) Index() []string {
	list := []string{}
	for _, opt := range strings.Split(f.Tag, ",") {
		opt = strings.TrimSpace(opt)
		m := IndexRegex.FindStringSubmatch(opt)
		if m != nil && len(m) == 5 {
			list = append(list, m[3])
		}
	}

	return list
}

//
// Get whether the field is a foreign key.
func (f *Field) Fk() *FK {
	for _, opt := range strings.Split(f.Tag, ",") {
		opt = strings.TrimSpace(opt)
		m := FkRegex.FindStringSubmatch(opt)
		if m != nil && len(m) == 6 {
			return &FK{
				Table: m[2],
				Field: m[4],
			}
		}
	}

	return nil
}

//
// Get whether field is auto-incremented.
func (f *Field) Incremented() bool {
	return f.hasOpt("incremented")
}

// Convert the specified `object` to a value
// (type) appropriate for the field.
func (f *Field) AsValue(object interface{}) (value interface{}, err error) {
	val := reflect.ValueOf(object)
	switch val.Kind() {
	case reflect.Ptr:
		val = val.Elem()
	case reflect.Struct,
		reflect.Slice,
		reflect.Map:
		err = liberr.Wrap(PredicateValueErr)
		return
	}
	switch f.Value.Kind() {
	case reflect.String:
		switch val.Kind() {
		case reflect.String:
			value = val.String()
		case reflect.Bool:
			b := val.Bool()
			value = strconv.FormatBool(b)
		case reflect.Int,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64:
			n := val.Int()
			value = strconv.FormatInt(n, 0)
		default:
			err = liberr.Wrap(PredicateValueErr)
		}
	case reflect.Bool:
		switch val.Kind() {
		case reflect.String:
			s := val.String()
			b, pErr := strconv.ParseBool(s)
			if pErr != nil {
				err = liberr.Wrap(pErr)
				return
			}
			value = b
		case reflect.Bool:
			value = val.Bool()
		case reflect.Int,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64:
			n := val.Int()
			if n != 0 {
				value = true
			} else {
				value = false
			}
		default:
			err = liberr.Wrap(PredicateValueErr)
		}
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		switch val.Kind() {
		case reflect.String:
			n, err := strconv.ParseInt(val.String(), 0, 64)
			if err != nil {
				err = liberr.Wrap(err)
			}
			value = n
		case reflect.Bool:
			if val.Bool() {
				value = 1
			} else {
				value = 0
			}
		case reflect.Int,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64:
			value = val.Int()
		default:
			err = liberr.Wrap(PredicateValueErr)
		}
	default:
		err = liberr.Wrap(FieldTypeErr)
	}

	return
}

//
// Get whether the field is `json` encoded.
func (f *Field) Encoded() (encoded bool) {
	switch f.Value.Kind() {
	case reflect.Struct,
		reflect.Slice,
		reflect.Map:
		encoded = true
	}

	return
}

//
// Detail level.
// Defaults:
//   0 = primary and natural fields.
//   1 = other fields.
func (f *Field) Detail() (level int) {
	level = 1
	for n := 0; n < MaxDetail; n++ {
		if f.hasOpt(fmt.Sprintf("d%d", n)) {
			level = n
			return
		}
	}
	if f.Pk() || f.Key() {
		level = 0
		return
	}

	return
}

//
// Match detail level.
func (f *Field) MatchDetail(level int) bool {
	return f.Detail() <= level
}

//
// Get whether field has an option.
func (f *Field) hasOpt(name string) bool {
	for _, opt := range strings.Split(f.Tag, ",") {
		opt = strings.TrimSpace(opt)
		if opt == name {
			return true
		}
	}

	return false
}

//
// FK constraint.
type FK struct {
	// Table name.
	Table string
	// Field name.
	Field string
}

//
// Get DDL.
func (f *FK) DDL(field *Field) string {
	return fmt.Sprintf(
		"FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE CASCADE",
		field.Name,
		f.Table,
		f.Field)
}

//
// Template data.
type TmplData struct {
	// Table name.
	Table string
	// Index name.
	Index string
	// Fields.
	Fields []*Field
	// Constraint DDL.
	Constraints []string
	// Natural key fields.
	Keys []*Field
	// Primary key.
	Pk *Field
	// List options.
	Options *ListOptions
	// Count
	Count bool
}

//
// Predicate
func (t TmplData) Predicate() Predicate {
	return t.Options.Predicate
}

//
// Pagination.
func (t TmplData) Page() *Page {
	return t.Options.Page
}

//
// Sort criteria
func (t TmplData) Sort() []int {
	return t.Options.Sort
}

//
// List options.
type ListOptions struct {
	// Pagination.
	Page *Page
	// Sort by field position.
	Sort []int
	// Field detail level.
	// Defaults:
	//   0 = primary and natural fields.
	//   1 = other fields.
	Detail int
	// Predicate
	Predicate Predicate
	// Table (name).
	table string
	// Fields.
	fields []*Field
	// Params.
	params []interface{}
}

//
// Validate options.
func (l *ListOptions) Build(table string, fields []*Field) error {
	l.table = table
	l.fields = fields
	if l.Predicate == nil {
		return nil
	}
	err := l.Predicate.Build(l)
	if err != nil {
		return err
	}

	return nil
}

//
// Get an appropriate parameter name.
// Builds a parameter and adds it to the options.param list.
func (l *ListOptions) Param(name string, value interface{}) string {
	name = fmt.Sprintf("%s%d", name, len(l.params))
	l.params = append(l.params, sql.Named(name, value))
	return ":" + name
}

//
// Fields filtered by detail level.
func (l *ListOptions) Fields() (filtered []*Field) {
	for _, f := range l.fields {
		if f.MatchDetail(l.Detail) {
			filtered = append(filtered, f)
		}
	}

	return
}

//
// Get params referenced by the predicate.
func (l *ListOptions) Params() []interface{} {
	return l.params
}
