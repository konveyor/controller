/*
File backing for collections.
File format:
   Length: 8 (uint64)
   | kind: 2 (uint16)
   | size: 8 (uint64)
   | object: n (gob encoded)
   | ...
*/
package filebacked

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/google/uuid"
	liberr "github.com/konveyor/controller/pkg/error"
	"io"
	"os"
	pathlib "path"
	"runtime"
)

//
// File extension.
const (
	Extension = ".fb"
)

//
// Working Directory.
var WorkingDir = "/tmp"

//
// Writer.
type Writer struct {
	// File path.
	path string
	// File.
	file *os.File
	// Number of objects written.
	length uint64
}

//
// Append (write) object.
func (w *Writer) Append(object interface{}) (err error) {
	// Lazy open.
	err = w.open()
	if err != nil {
		return
	}
	// Update catalog.
	kind := catalog.add(object)
	// Encode object.
	var bfr bytes.Buffer
	encoder := gob.NewEncoder(&bfr)
	err = encoder.Encode(object)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	// Write entry.
	err = w.writeEntry(kind, bfr)

	return
}

//
// Get a reader.
func (w *Writer) Reader() (reader *Reader) {
	path := w.newPath()
	err := w.file.Sync()
	if err == nil {
		err = os.Link(w.path, path)
	}
	reader = &Reader{
		error: liberr.Wrap(err),
		path:  path,
	}
	runtime.SetFinalizer(
		reader,
		func(r *Reader) {
			r.Close()
		})

	return
}

//
// Close the writer.
func (w *Writer) Close() {
	if w.file != nil {
		_ = w.file.Close()
		_ = os.Remove(w.path)
		w.file = nil
	}
}

//
// Open the writer.
func (w *Writer) open() (err error) {
	if w.file != nil {
		return
	}
	w.path = w.newPath()
	w.file, err = os.Create(w.path)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	err = w.writeLength()
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// Write entry.
func (w *Writer) writeEntry(kind uint16, bfr bytes.Buffer) (err error) {
	file := w.file
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
	// Write length.
	w.length++
	err = w.writeLength()
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// Write length.
// Number of objects written.
func (w *Writer) writeLength() (err error) {
	_, err = w.file.Seek(0, io.SeekStart)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, w.length)
	_, err = w.file.Write(b)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	_, err = w.file.Seek(0, io.SeekEnd)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// New path.
func (w *Writer) newPath() string {
	uid, _ := uuid.NewUUID()
	name := uid.String() + Extension
	return pathlib.Join(WorkingDir, name)
}

//
// Reader.
type Reader struct {
	// Error
	error error
	// File path.
	path string
	// File.
	file *os.File
}

//
// Length.
// Number of objects in the list.
func (r *Reader) Len() (length int) {
	// Lazy open.
	err := r.open()
	if err != nil {
		return
	}
	n, _ := r.len()
	length = int(n)
	return
}

//
// Error.
func (r *Reader) Error() error {
	return r.error
}

//
// Get the next object.
func (r *Reader) NextWith(object interface{}) (hasNext bool, err error) {
	if r.error != nil {
		return
	}
	defer func() {
		r.error = err
	}()
	// Lazy open.
	err = r.open()
	if err != nil {
		return
	}
	// Read entry.
	hasNext, _, b, err := r.readEntry()
	if !hasNext || err != nil {
		return
	}
	// Decode object.
	bfr := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bfr)
	err = decoder.Decode(object)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// Get the next object.
func (r *Reader) Next() (object interface{}, hasNext bool, err error) {
	if r.error != nil {
		return
	}
	defer func() {
		r.error = err
	}()
	// Lazy open.
	err = r.open()
	if err != nil {
		return
	}
	// Read entry.
	hasNext, kind, b, err := r.readEntry()
	if !hasNext || err != nil {
		return
	}
	// Decode object.
	bfr := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bfr)
	object, found := catalog.build(kind)
	if !found {
		err = liberr.New("kind not found in the catalog.")
		return
	}
	err = decoder.Decode(object)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// Read next entry.
func (r *Reader) readEntry() (hasNext bool, kind uint16, bfr []byte, err error) {
	file := r.file
	// Read object kind.
	b := make([]byte, 2)
	_, err = file.Read(b)
	if err != nil {
		if err != io.EOF {
			err = liberr.Wrap(err)
		} else {
			err = nil
		}
		return
	}
	kind = binary.LittleEndian.Uint16(b)
	// Read object encoded length.
	b = make([]byte, 8)
	_, err = file.Read(b)
	if err != nil {
		if err != io.EOF {
			err = liberr.Wrap(err)
		} else {
			err = nil
		}
		return
	}
	n := int64(binary.LittleEndian.Uint64(b))
	// Read encoded object.
	b = make([]byte, n)
	_, err = file.Read(b)
	if err != nil {
		if err != io.EOF {
			err = liberr.Wrap(err)
		} else {
			err = nil
		}
		return
	}

	hasNext = true
	bfr = b

	return
}

//
// Open the reader.
func (r *Reader) open() (err error) {
	if r.file != nil {
		return
	}
	// Open.
	r.file, err = os.Open(r.path)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	// Skip past length.
	_, err = r.file.Seek(8, io.SeekStart)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	return
}

//
// Close the reader.
func (r *Reader) Close() {
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
	// Unlink.
	_ = os.Remove(r.path)
}

//
// Read length.
// Number of objects.
func (r *Reader) len() (length uint64, err error) {
	file := r.file
	// Note current position.
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	defer func() {
		_, err = file.Seek(offset, io.SeekStart)
		if err != nil {
			err = liberr.Wrap(err)
		}
	}()
	// Seek to beginning of the file.
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	// Read length.
	b := make([]byte, 8)
	_, err = file.Read(b)
	if err != nil {
		if err != io.EOF {
			err = liberr.Wrap(err)
		} else {
			err = nil
		}
		return
	}

	length = binary.LittleEndian.Uint64(b)

	return
}
