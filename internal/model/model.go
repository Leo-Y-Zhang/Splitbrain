// Package model holds the sequential specifications a history is checked
// against. A model says what a single object would do if every operation
// happened one at a time, in some order, with no concurrency at all. The
// checker's job is to decide whether such an order exists that both matches the
// model and respects the real times in the history.
package model

import (
	"fmt"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// A State is an opaque model state.
//
// It must be comparable with == and usable as a map key, because the search
// caches states it has already reached. That rules out slices and maps: a model
// that needs one of those has to encode it into a string or an array first.
type State any

// A Model is the sequential specification of one object.
type Model interface {
	// Name is the identifier used on the command line.
	Name() string

	// Init is the state of a freshly created object.
	Init() State

	// Step applies op to s.
	//
	// It returns the state afterwards and whether the transition is
	// consistent with what the client saw. When op.Outcome is history.Info
	// the client saw nothing, so no output can be contradicted and only the
	// state transition applies - but the operation still counts as having
	// happened at that point, which is the whole reason indeterminate
	// operations cannot simply be discarded.
	Step(s State, op history.Op) (State, bool)

	// Describe renders a state for a counterexample report.
	Describe(s State) string

	// Explain says, in one line, why op could not be applied to s. It is only
	// called after Step has returned false, and only to write a report.
	Explain(s State, op history.Op) string
}

// Registers start at zero rather than at "absent". A dedicated absent state
// buys nothing here - it doubles the model's cases while testing the same
// orderings - and starting from a known value means a read before any write is
// still a constraint rather than a free pass.
const InitialValue = 0

// CASRegister is a single integer register supporting read, write and
// compare-and-swap. It is the model that makes concurrency bugs visible:
// read and write alone are so permissive that many broken stores pass, whereas
// a CAS that reports it swapped is a claim about the exact value the register
// held at that instant.
type CASRegister struct{}

// Name identifies the model on the command line.
func (CASRegister) Name() string { return "cas-register" }

// Init returns the register's starting value.
func (CASRegister) Init() State { return InitialValue }

// Step applies op to the register.
func (CASRegister) Step(s State, op history.Op) (State, bool) {
	v, okState := s.(int)
	if !okState {
		return s, false
	}
	known := op.Outcome == history.OK

	switch op.Kind {
	case history.Read:
		if known && op.Observed != v {
			return s, false
		}
		return v, true

	case history.Write:
		return op.Value, true

	case history.CAS:
		swapped := v == op.From
		if known && op.Swapped != swapped {
			return s, false
		}
		if swapped {
			return op.To, true
		}
		return v, true

	default:
		return s, false
	}
}

// Describe renders the register's value.
func (CASRegister) Describe(s State) string { return fmt.Sprintf("%v", s) }

// Explain says why op did not fit.
func (CASRegister) Explain(s State, op history.Op) string {
	switch op.Kind {
	case history.Read:
		return fmt.Sprintf("read returned %d, but the register held %v at that point", op.Observed, s)
	case history.CAS:
		if op.Swapped {
			return fmt.Sprintf("cas reported it swapped %d to %d, but the register held %v", op.From, op.To, s)
		}
		return fmt.Sprintf("cas reported it did not swap because the register was not %d, but it held %v", op.From, s)
	default:
		return fmt.Sprintf("%s does not fit a register holding %v", op.Kind, s)
	}
}

// Register is a read/write register with no compare-and-swap.
//
// It exists to be the wrong answer as much as the right one: running a history
// that contains CAS operations against this model is an error rather than a
// pass, because a checker that quietly ignores operations it does not model
// reports a verdict about a system nobody was testing.
type Register struct{}

// Name identifies the model on the command line.
func (Register) Name() string { return "register" }

// Init returns the register's starting value.
func (Register) Init() State { return InitialValue }

// Step applies op, refusing compare-and-swap.
func (Register) Step(s State, op history.Op) (State, bool) {
	if op.Kind == history.CAS {
		return s, false
	}
	return CASRegister{}.Step(s, op)
}

// Describe renders the register's value.
func (Register) Describe(s State) string { return CASRegister{}.Describe(s) }

// Explain says why op did not fit.
func (Register) Explain(s State, op history.Op) string {
	if op.Kind == history.CAS {
		return "this history contains compare-and-swap, which the plain register model does not describe; use -model cas-register"
	}
	return CASRegister{}.Explain(s, op)
}

// ByName returns the model with the given command-line name.
func ByName(name string) (Model, error) {
	for _, m := range All() {
		if m.Name() == name {
			return m, nil
		}
	}
	return nil, fmt.Errorf("unknown model %q (have: cas-register, register)", name)
}

// All lists every model, in the order they should be offered.
func All() []Model { return []Model{CASRegister{}, Register{}} }
