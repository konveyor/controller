package model

import (
	liberr "github.com/konveyor/controller/pkg/error"
	fb "github.com/konveyor/controller/pkg/filebacked"
	"github.com/konveyor/controller/pkg/ref"
	"sync"
)

//
// Event Actions.
var (
	Parity  uint8 = 0x00
	Error   uint8 = 0x01
	End     uint8 = 0x02
	Created uint8 = 0x10
	Updated uint8 = 0x20
	Deleted uint8 = 0x40
)

//
// Model event.
type Event struct {
	// The event subject.
	Model Model
	// The event action (created|updated|deleted).
	Action uint8
	// The updated model.
	Updated Model
}

//
// Event handler.
type EventHandler interface {
	// Watch has started.
	Started()
	// Parity marker.
	// The watch has delivered the initial set
	// of `Created` events.
	Parity()
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
	queue chan fb.Iterator
	// Journal.
	journal *Journal
	// Started
	started bool
	// Done
	done bool
}

//
// End the watch.
func (w *Watch) End() {
	w.journal.End(w)
}

//
// The watch has not ended.
func (w *Watch) Alive() bool {
	return !w.done
}

//
// Match by model `kind`.
func (w *Watch) Match(model Model) bool {
	return ref.ToKind(w.Model) == ref.ToKind(model)
}

//
// Queue event.
func (w *Watch) notify(itr fb.Iterator) {
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
		defer func() {
			w.started = false
			w.done = true
			w.Handler.End()
		}()
		count := 0
		for itr := range w.queue {
			for {
				event := Event{}
				event, hasNext, err := w.next(itr)
				if err != nil {
					w.Handler.Error(err)
					break
				}
				if !hasNext {
					break
				}
				if !w.Match(event.Model) {
					continue
				}
				switch event.Action {
				case Created:
					w.Handler.Created(event)
				case Updated:
					w.Handler.Updated(event)
				case Deleted:
					w.Handler.Deleted(event)
				default:
					w.Handler.Error(liberr.New("unknown action"))
				}
			}
			count++
			itr.Close()
			if count == 1 {
				w.Handler.Parity()
			}
		}
	}

	w.started = true
	go run()
}

//
// Next Event from iterator.
func (w *Watch) next(itr fb.Iterator) (event Event, hasNext bool, err error) {
	event = Event{}
	// Event.
	hasNext, err = itr.NextWith(&event)
	if err != nil {
		return
	}
	if !hasNext {
		return
	}
	// Event.Object.
	object, hasNext, err := itr.Next()
	if err != nil {
		return
	}
	if !hasNext {
		err = liberr.New("model expected after event.")
		return
	}
	if model, cast := object.(Model); cast {
		event.Model = model
	} else {
		w.Handler.Error(err)
		return
	}
	if event.Action == Updated {
		// Event.Updated .
		object, hasNext, err = itr.Next()
		if err != nil {
			return
		}
		if !hasNext {
			err = liberr.New("model expected after event.")
			return
		}
		if model, cast := object.(Model); cast {
			event.Updated = model
		} else {
			w.Handler.Error(err)
			return
		}
	}

	return
}

//
// Terminate.
func (w *Watch) terminate() {
	if w.started {
		close(w.queue)
	}
}

//
// DB journal.
// Provides model watch events.
type Journal struct {
	mutex sync.RWMutex
	// List of registered watches.
	watchList []*Watch
	// Recorded (staged) event list.
	// file-backed list of:
	//   Event, model, ..,
	//   Event, model, ..,
	staged fb.List
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
		journal: r,
	}
	r.watchList = append(r.watchList, watch)
	watch.queue = make(chan fb.Iterator, 250)
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
		w.terminate()
	}

	r.watchList = kept
}

//
// A model has been created.
// Record the event in the staged list.
func (r *Journal) Created(model Model, committed bool) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	// Event.
	err = r.staged.Append(
		Event{
			Action: Created,
		})
	if err != nil {
		return
	}
	// Event.Model.
	err = r.staged.Append(model)
	if err != nil {
		return
	}
	if committed {
		r.committed()
	}

	return
}

//
// A model has been updated.
// Record the event in the staged list.
func (r *Journal) Updated(model Model, updated Model, committed bool) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	// Event.
	err = r.staged.Append(
		Event{
			Action: Updated,
		})
	if err != nil {
		return
	}
	// Event.Model.
	err = r.staged.Append(model)
	if err != nil {
		return
	}
	// Event.Updated.
	err = r.staged.Append(updated)
	if err != nil {
		return
	}
	if committed {
		r.committed()
	}

	return
}

//
// A model has been deleted.
// Record the event in the staged list.
func (r *Journal) Deleted(model Model, committed bool) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hasWatch(model) {
		return
	}
	// Event.
	err = r.staged.Append(
		Event{
			Action: Deleted,
		})
	if err != nil {
		return
	}
	// Event.Model.
	err = r.staged.Append(model)
	if err != nil {
		return
	}
	if committed {
		r.committed()
	}

	return
}

//
// Transaction committed.
// Recorded (staged) events are forwarded to watches.
func (r *Journal) Committed() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.committed()
}

//
// Reset the journal.
// Discard recorded (staged) events.
func (r *Journal) Reset() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.staged.Close()
	r.staged = fb.List{}
}

//
// Close the journal.
// End all watches.
func (r *Journal) Close() (err error) {
	r.staged.Close()
	for _, w := range r.watchList {
		r.End(w)
	}

	return
}

//
// Transaction committed.
// Recorded (staged) events forwarded to watches.
func (r *Journal) committed() {
	for _, w := range r.watchList {
		itr := r.staged.Iter()
		w.notify(itr)
	}

	r.staged.Close()
	r.staged = fb.List{}
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
// Stock event handler.
// Provides default event methods.
type StockEventHandler struct{}

//
// Watch has started.
func (r *StockEventHandler) Started() {}

//
// Watch has parity.
func (r *StockEventHandler) Parity() {}

//
// A model has been created.
func (r *StockEventHandler) Created(Event) {}

//
// A model has been updated.
func (r *StockEventHandler) Updated(Event) {}

//
// A model has been deleted.
func (r *StockEventHandler) Deleted(Event) {}

//
// An error has occurred delivering an event.
func (r *StockEventHandler) Error(error) {}

//
// An event watch has ended.
func (r *StockEventHandler) End() {}
