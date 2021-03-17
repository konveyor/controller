/*
Provides file-backed queue.

//
// New queue.
q := fbq.New("/tmp")
defer q.Close()

//
// Enqueue an object.
err := q.Put(object)

//
// Drain the queue.
for {
    object, hasNext, err := q.Next()
    if err != nil || !hasNext {
        break
    }
}
*/
package fbq
