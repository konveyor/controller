/*
Provides file-backed list.

//
// New list.
list := fb.List{}
defer list.Close()

//
// Append an object.
err := list.Append(object)

//
// Iterate the list.
itr := list.Iter()
defer itr.Close()
for {
    object, hasNext, err := itr.Next()
    if err != nil || !hasNext {
        break
    }
}

//
// Iterate the list.
itr := list.Iter()
defer itr.Close()
for {
    person := Person{}
    hasNext, err := itr.NextWith(&person))
    if err != nil || !hasNext {
        break
    }
}
*/
package filebacked

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
