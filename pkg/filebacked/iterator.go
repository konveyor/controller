package filebacked

//
// Iterator.
// Read-only collection with stateful iteration.
type Iterator interface {
	// Number of items.
	Len() int
	// Object at index.
	At(index int) interface{}
	// Object at index (with).
	AtWith(int, interface{})
	// Next object.
	Next() (interface{}, bool)
	// Next object (with).
	NextWith(object interface{}) bool
	// Close the iterator.
	Close()
}

//
// Iterator.
type FbIterator struct {
	// Reader.
	*Reader
	// Current position.
	current int
}

//
// Next object.
func (r *FbIterator) Next() (object interface{}, hasNext bool) {
	if r.current < r.Len() {
		object = r.At(r.current)
		r.current++
		hasNext = true
	}

	return
}

//
// Next object.
func (r *FbIterator) NextWith(object interface{}) (hasNext bool) {
	if r.current < r.Len() {
		r.AtWith(r.current, object)
		r.current++
		hasNext = true
	}

	return
}

//
// Empty.
type EmptyIterator struct {
}

//
// Length.
func (*EmptyIterator) Len() int {
	return 0
}

// Object at index.
func (*EmptyIterator) At(int) interface{} {
	return nil
}

// Object at index.
func (*EmptyIterator) AtWith(int, interface{}) {
	return
}

//
// Next object.
func (*EmptyIterator) Next() (interface{}, bool) {
	return nil, false
}

//
// Next object.
func (*EmptyIterator) NextWith(object interface{}) bool {
	return false
}

//
// Close the iterator.
func (*EmptyIterator) Close() {
}
