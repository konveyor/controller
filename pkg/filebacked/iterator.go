package filebacked

//
// Iterator.
type Iterator interface {
	// Length.
	Len() int
	// Next object.
	Next() (interface{}, bool, error)
	// Next object.
	NextWith(object interface{}) (bool, error)
	// Get associated error.
	Error() error
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
func (*EmptyIterator) Next() (interface{}, bool, error) {
	return nil, false, nil
}

//
// Next object.
func (*EmptyIterator) NextWith(object interface{}) (bool, error) {
	return false, nil
}

//
// Get associated error.
func (*EmptyIterator) Error() error {
	return nil
}

//
// Close the iterator.
func (*EmptyIterator) Close() {
}
