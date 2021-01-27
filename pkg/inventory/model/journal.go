package model

import (
	liberr "github.com/konveyor/controller/pkg/error"
	"github.com/konveyor/controller/pkg/ref"
	"reflect"
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
	queue chan *Event
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
func (w *Watch) notify(event *Event) {
	if !w.Match(event.Model) {
		return
	}
	defer func() {
		recover()
	}()
	select {
	case w.queue <- event:
	default:
		err := liberr.New("full queue, event discarded")
		w.Handler.Error(err)
	}
}

//
// Run the watch.
// Forward events to the `handler`.
func (w *Watch) Start(list *reflect.Value) {
	if w.started {
		return
	}
	run := func() {
		for i := 0; i < list.Len(); i++ {
			m := list.Index(i).Addr().Interface()
			w.Handler.Created(
				Event{
					Model:  m.(Model),
					Action: Created,
				})
		}
		list = nil
		for event := range w.queue {
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
	// List of registered watches.
	watchList []*Watch
	// Queue of staged events.
	staged []*Event
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
	watch.queue = make(chan *Event, 10000)
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
func (r *Journal) Created(model Model) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	r.staged = append(
		r.staged,
		&Event{
			Model:  Clone(model),
			Action: Created,
		})
}

//
// A model has been updated.
// Queue an event.
func (r *Journal) Updated(model Model, updated Model) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	r.staged = append(
		r.staged,
		&Event{
			Model:   Clone(model),
			Updated: Clone(updated),
			Action:  Updated,
		})
}

//
// A model has been deleted.
// Queue an event.
func (r *Journal) Deleted(model Model) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	r.staged = append(
		r.staged,
		&Event{
			Model:  Clone(model),
			Action: Deleted,
		})
}

//
// Commit staged events and notify handlers.
func (r *Journal) Commit() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, event := range r.staged {
		for _, w := range r.watchList {
			w.notify(event)
		}
	}

	r.staged = []*Event{}
}

//
// Discard staged events.
func (r *Journal) Unstage() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.staged = []*Event{}
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
