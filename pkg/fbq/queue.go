package fbq

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/google/uuid"
	liberr "github.com/konveyor/controller/pkg/error"
	"io"
	"os"
	pathlib "path"
	"reflect"
)

const (
	Extension = ".fbq"
)

//
// New file-backed queue at the specified path.
func New(dir string) *Queue {
	uid, _ := uuid.NewUUID()
	name := uid.String() + Extension
	path := pathlib.Join(dir, name)
	return &Queue{
		path: path,
		writer: Writer{
			path: path,
		},
	}
}

//
// Iterator.
type Iterator interface {
	// Next object.
	Next() (interface{}, bool, error)
	// Get associated error.
	Error() error
	// Close the iterator.
	Close()
}

//
// File-based queue.
type Queue struct {
	// File path.
	path string
	// Queue writer.
	writer Writer
	// Queue iterator.
	iterator Iterator
}

//
// Enqueue object.
func (q *Queue) Put(object interface{}) (err error) {
	err = q.writer.Put(object)
	return
}

//
// Dequeue the next object.
func (q *Queue) Next() (object interface{}, hasNext bool, err error) {
	if q.iterator == nil {
		q.iterator = q.Iterator()
	}
	object, hasNext, err = q.iterator.Next()
	return
}

//
// Get an iterator.
func (q *Queue) Iterator() (itr Iterator) {
	uid, _ := uuid.NewUUID()
	name := uid.String() + Extension
	path := pathlib.Join(pathlib.Dir(q.path), name)
	err := os.Link(q.path, path)
	itr = &Reader{
		catalog: &q.writer.catalog,
		error:   err,
		path:    path,
	}

	return
}

//
// Close the queue.
func (q *Queue) Close(delete bool) {
	q.writer.Close(delete)
	if q.iterator != nil {
		q.iterator.Close()
	}
}

//
// Writer.
type Writer struct {
	// File path.
	path string
	// Catalog of object types.
	catalog []interface{}
	// File.
	file *os.File
}

//
// Enqueue object.
func (w *Writer) Put(object interface{}) (err error) {
	// Lazy open.
	if w.file == nil {
		err = w.open()
		if err != nil {
			return
		}
	}
	file := w.file
	// Encode object and add to catalog.
	var bfr bytes.Buffer
	encoder := gob.NewEncoder(&bfr)
	err = encoder.Encode(object)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	kind := w.add(object)
	// Write object kind.
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, kind)
	_, err = file.Write(b)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	// Write object encoded length.
	n := bfr.Len()
	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(n))
	_, err = file.Write(b)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	// Write encoded object.
	nWrite, err := file.Write(bfr.Bytes())
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	if n != nWrite {
		err = liberr.New("Write failed.")
	}

	_ = file.Sync()

	return
}

//
// Close the writer.
func (w *Writer) Close(delete bool) {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
		if delete {
			_ = os.Remove(w.path)
		}
	}
}

//
// Open the writer.
func (w *Writer) open() (err error) {
	if w.file != nil {
		return
	}
	w.file, err = os.Create(w.path)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// Add object (proto) to the catalog.
func (w *Writer) add(object interface{}) (kind uint16) {
	t := reflect.TypeOf(object)
	for i, f := range w.catalog {
		if t == reflect.TypeOf(f) {
			kind = uint16(i)
			return
		}
	}

	kind = uint16(len(w.catalog))
	w.catalog = append(w.catalog, object)
	return
}

//
// Reader.
type Reader struct {
	// Error
	error error
	// File path.
	path string
	// Catalog of object types.
	catalog *[]interface{}
	// File.
	file *os.File
}

//
// Error.
func (r *Reader) Error() error {
	return r.error
}

//
// Dequeue the next object.
func (r *Reader) Next() (object interface{}, hasNext bool, err error) {
	defer func() {
		err = r.error
	}()
	if r.error != nil {
		return
	}
	// Lazy open.
	if r.file == nil {
		r.error = r.open()
		if r.error != nil {
			return
		}
	}
	file := r.file
	// Read object kind.
	b := make([]byte, 2)
	_, r.error = file.Read(b)
	if r.error != nil {
		if r.error != io.EOF {
			r.error = liberr.Wrap(r.error)
		} else {
			r.error = nil
		}
		return
	}
	// Read object encoded length.
	kind := binary.LittleEndian.Uint16(b)
	b = make([]byte, 8)
	_, r.error = file.Read(b)
	if r.error != nil {
		if r.error != io.EOF {
			r.error = liberr.Wrap(r.error)
		} else {
			r.error = nil
		}
		return
	}
	// Read encoded object.
	n := int64(binary.LittleEndian.Uint64(b))
	b = make([]byte, n)
	_, r.error = file.Read(b)
	if r.error != nil {
		if r.error != io.EOF {
			r.error = liberr.Wrap(r.error)
		} else {
			r.error = nil
		}
		return
	}
	// Decode object.
	bfr := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bfr)
	object, found := r.find(kind)
	if !found {
		r.error = liberr.New("unknown kind")
		return
	}
	r.error = decoder.Decode(object)
	if r.error != nil {
		r.error = liberr.Wrap(r.error)
		return
	}

	hasNext = true

	return
}

//
// Close the reader.
func (r *Reader) Close() {
	if r.file != nil {
		_ = r.file.Close()
		_ = os.Remove(r.path)
		r.file = nil
	}
}

//
// Open the reader.
func (r *Reader) open() (err error) {
	if r.file != nil {
		return
	}
	r.file, err = os.Open(r.path)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// Find object (kind) in the catalog.
func (r *Reader) find(kind uint16) (object interface{}, found bool) {
	catalog := *r.catalog
	i := int(kind)
	if i < len(catalog) {
		object = catalog[i]
		object = reflect.New(reflect.TypeOf(object)).Interface()
		found = true
	}

	return
}
