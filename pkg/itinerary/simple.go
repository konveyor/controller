package itinerary

import liberr "github.com/konveyor/controller/pkg/error"

//
// List of steps.
type Pipeline []Step

//
// Predicate flag.
type Flag = int16

//
// Predicate.
// Flags delegated to the predicate.
type Predicate interface {
	Allowed(Flag) (bool, error)
}

//
// Itinerary step.
type Step struct {
	// Name.
	Name string
	// Any of these conditions be satisfied for
	// the step to be included.
	All Flag
	// All of these conditions be satisfied for
	// the step to be included.
	Any Flag
}

//
// An itinerary.
// List of conditional steps.
type Itinerary struct {
	// Pipeline (list) of steps.
	Pipeline
	// Predicate.
	Predicate
	// Name.
	Name string
}

//
// Errors.
var (
	StepNotFound = liberr.New("step not found")
)

//
// Get the current step.
func (r *Itinerary) Get(name string) (*Step, error) {
	for i := 0; i < len(r.Pipeline); i++ {
		step := &r.Pipeline[i]
		if step.Name == name {
			return step, nil
		}
	}

	return nil, StepNotFound
}

//
// Get the next step in the itinerary.
func (r *Itinerary) Next(name string) (next *Step, done bool, err error) {
	current := -1
	for i := 0; i < len(r.Pipeline); i++ {
		step := &r.Pipeline[i]
		if step.Name == name {
			current = i
		}
	}
	if current == -1 {
		err = StepNotFound
		return
	}
	for i := current + 1; i < len(r.Pipeline); i++ {
		step := &r.Pipeline[i]
		allowed, pErr := r.hasAny(step)
		if pErr != nil {
			err = pErr
			return
		}
		if !allowed {
			continue
		}
		allowed, pErr = r.hasAll(step)
		if pErr != nil {
			err = pErr
			return
		}
		if !allowed {
			continue
		}

		next = step
		return
	}

	done = true
	return
}

//
// The step has satisfied ANY of the predicates.
func (r *Itinerary) hasAny(step *Step) (allowed bool, err error) {
	for i := 0; i < 16; i++ {
		flag := Flag(1 << i)
		if (step.Any & flag) == 0 {
			continue
		}
		if r.Predicate == nil {
			continue
		}
		allowed, err = r.Predicate.Allowed(flag)
		if allowed || err != nil {
			return
		}
	}

	allowed = true
	return
}

//
// The step has satisfied ALL of the predicates.
func (r *Itinerary) hasAll(step *Step) (allowed bool, err error) {
	for i := 0; i < 16; i++ {
		flag := Flag(1 << i)
		if (step.All & flag) == 0 {
			continue
		}
		if r.Predicate == nil {
			continue
		}
		allowed, err = r.Predicate.Allowed(flag)
		if !allowed || err != nil {
			return
		}
	}

	allowed = true
	return
}
