package model

import (
	liberr "github.com/konveyor/controller/pkg/error"
	"github.com/konveyor/controller/pkg/fbq"
	"github.com/konveyor/controller/pkg/ref"
	"sync"
)

//
// Event Actions.
var (
	Created int8 = 0x01
	Updated int8 = 0x02
	Deleted int8 = 0x04
)

//
// Model event.
type Event struct {
	// The event subject.
	Model Model
	// The event action (created|updated|deleted).
	Action int8
	// The updated model.
	Updated Model
}

//
// Event handler.
type EventHandler interface {
	// Watch has started.
	Started()
	// A model has been created.
	Created(Event)
	// A model has been updated.
	Updated(Event)
	// A model has been deleted.
	Deleted(Event)
	// An error has occurred delivering an event.
	Error(error)
	// An event watch has ended.
	End()
}

//
// Model event watch.
type Watch struct {
	// Model to be watched.
	Model Model
	// Event handler.
	Handler EventHandler
	// Event queue.
	queue chan fbq.Iterator
	// Started
	started bool
}

//
// Match by model `kind`.
func (w *Watch) Match(model Model) bool {
	return ref.ToKind(w.Model) == ref.ToKind(model)
}

//
// Queue event.
func (w *Watch) notify(itr fbq.Iterator) {
	defer func() {
		recover()
	}()
	select {
	case w.queue <- itr:
	default:
		itr.Close()
		err := liberr.New("full queue, event discarded")
		w.Handler.Error(err)
	}
}

//
// Run the watch.
// Forward events to the `handler`.
func (w *Watch) Start() {
	if w.started {
		return
	}
	w.Handler.Started()
	run := func() {
		for itr := range w.queue {
			for {
				object, hasNext, err := itr.Next()
				if !hasNext {
					break
				}
				if err != nil {
					w.Handler.Error(err)
					break
				}
				event := object.(*Event)
				if !w.Match(event.Model) {
					continue
				}
				switch event.Action {
				case Created:
					w.Handler.Created(*event)
				case Updated:
					w.Handler.Updated(*event)
				case Deleted:
					w.Handler.Deleted(*event)
				default:
					w.Handler.Error(liberr.New("unknown action"))
				}
			}
			itr.Close()
		}
		w.Handler.End()
	}

	w.started = true
	go run()
}

//
// End the watch.
func (w *Watch) End() {
	close(w.queue)
}

//
// Event manager.
type Journal struct {
	mutex sync.RWMutex
	// Path.
	path string
	// List of registered watches.
	watchList []*Watch
	// Event writer.
	writer *fbq.Queue
}

//
// Watch a `watch` of the specified model.
// The returned watch has not been started.
// See: Watch.Start().
func (r *Journal) Watch(model Model, handler EventHandler) (*Watch, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	watch := &Watch{
		Handler: handler,
		Model:   model,
	}
	r.watchList = append(r.watchList, watch)
	watch.queue = make(chan fbq.Iterator, 100)
	return watch, nil
}

//
// End watch.
func (r *Journal) End(watch *Watch) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	kept := []*Watch{}
	for _, w := range r.watchList {
		if w != watch {
			kept = append(kept, w)
			continue
		}
		w.End()
	}

	r.watchList = kept
}

//
// A model has been created.
// Queue an event.
func (r *Journal) Created(model Model) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	if r.writer == nil {
		r.writer = fbq.New(r.path)
	}
	err = r.writer.Put(
		Event{
			Model:  Clone(model),
			Action: Created,
		})

	return
}

//
// A model has been updated.
// Queue an event.
func (r *Journal) Updated(model Model, updated Model) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	if r.writer == nil {
		r.writer = fbq.New(r.path)
	}
	err = r.writer.Put(
		Event{
			Model:   Clone(model),
			Updated: Clone(updated),
			Action:  Updated,
		})

	return
}

//
// A model has been deleted.
// Queue an event.
func (r *Journal) Deleted(model Model) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	if r.writer == nil {
		r.writer = fbq.New(r.path)
	}
	err = r.writer.Put(
		Event{
			Model:  Clone(model),
			Action: Deleted,
		})

	return
}

//
// Commit staged events and notify handlers.
func (r *Journal) Commit() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.writer == nil {
		return
	}
	for _, w := range r.watchList {
		itr := r.writer.Iterator()
		w.notify(itr)
	}

	r.writer.Close()
	r.writer = nil
}

//
// Discard staged events.
func (r *Journal) Unstage() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.writer.Close()
	r.writer = nil
}

//
// Model is being watched.
// Determine if there a watch interested in the model.
func (r *Journal) hasWatch(model Model) bool {
	for _, w := range r.watchList {
		if w.Match(model) {
			return true
		}
	}

	return false
}

//
// Stub event handler.
type StubEventHandler struct{}

//
// Watch has started.
func (r *StubEventHandler) Started() {}

//
// A model has been created.
func (r *StubEventHandler) Created(Event) {}

//
// A model has been updated.
func (r *StubEventHandler) Updated(Event) {}

//
// A model has been deleted.
func (r *StubEventHandler) Deleted(Event) {}

//
// An error has occurred delivering an event.
func (r *StubEventHandler) Error(error) {}

//
// An event watch has ended.
func (r *StubEventHandler) End() {}
