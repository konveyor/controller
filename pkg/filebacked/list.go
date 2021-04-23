/*
Provides file-backed list.

//
// New list.
list := fb.NewList()

//
// Append an object.
err := list.Append(object)

//
// Iterate the list.
itr := list.Iter()
for {
    object, hasNext, err := itr.Next()
    if err != nil || !hasNext {
        break
    }
}

//
// Iterate the list.
itr := list.Iter()
for {
    person := Person{}
    hasNext, err := itr.NextWith(&person))
    if err != nil || !hasNext {
        break
    }
}
*/
package filebacked

import "runtime"

//
// List factory.
func NewList() (list *List) {
	list = &List{}
	runtime.SetFinalizer(
		list,
		func(l *List) {
			l.Close()
		})
	return
}

//
// File-backed list.
type List struct {
	// File writer.
	writer Writer
}

//
// Append an object.
func (l *List) Append(object interface{}) (err error) {
	err = l.writer.Append(object)
	return
}

//
// Length.
// Number of objects.
func (l *List) Len() int {
	return int(l.writer.length)
}

//
// Get an iterator.
func (l *List) Iter() (itr Iterator) {
	if l.Len() > 0 {
		itr = l.writer.Reader()
	} else {
		itr = &EmptyIterator{}
	}

	return
}

//
// Close (delete) the list.
func (l *List) Close() {
	l.writer.Close()
}
