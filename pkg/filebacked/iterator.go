package filebacked

//
// Iterator.
type Iterator interface {
	// Length.
	Len() int
	// Next object.
	Next() (interface{}, bool)
	// Next object.
	NextWith(object interface{}) bool
	// Close the iterator.
	Close()
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
