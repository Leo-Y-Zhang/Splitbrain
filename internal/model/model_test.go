package model

import (
	"testing"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

func read(observed int, oc history.Outcome) history.Op {
	return history.Op{Key: "x", Kind: history.Read, Observed: observed, Outcome: oc}
}

func write(v int, oc history.Outcome) history.Op {
	return history.Op{Key: "x", Kind: history.Write, Value: v, Outcome: oc}
}

func cas(from, to int, swapped bool, oc history.Outcome) history.Op {
	return history.Op{Key: "x", Kind: history.CAS, From: from, To: to, Swapped: swapped, Outcome: oc}
}

func TestCASRegisterRead(t *testing.T) {
	m := CASRegister{}
	s := m.Init()
	if s != InitialValue {
		t.Fatalf("Init() = %v, want %v", s, InitialValue)
	}

	if next, ok := m.Step(s, read(0, history.OK)); !ok || next != 0 {
		t.Fatalf("reading the initial value must be accepted and must not change the state; got %v/%v", next, ok)
	}
	if _, ok := m.Step(s, read(1, history.OK)); ok {
		t.Fatal("a read of 1 from a register holding 0 must be rejected")
	}
}

func TestCASRegisterWrite(t *testing.T) {
	m := CASRegister{}
	next, ok := m.Step(m.Init(), write(7, history.OK))
	if !ok || next != 7 {
		t.Fatalf("write(7) gave %v/%v, want 7/true", next, ok)
	}
}

func TestCASRegisterSwap(t *testing.T) {
	m := CASRegister{}
	s, _ := m.Step(m.Init(), write(1, history.OK))

	next, ok := m.Step(s, cas(1, 2, true, history.OK))
	if !ok || next != 2 {
		t.Fatalf("an honest successful cas gave %v/%v, want 2/true", next, ok)
	}

	next, ok = m.Step(s, cas(5, 2, false, history.OK))
	if !ok || next != 1 {
		t.Fatalf("an honest failed cas must leave the register alone; got %v/%v", next, ok)
	}
}

func TestCASRegisterCatchesLyingCAS(t *testing.T) {
	// These two are the reason cas is worth generating at all: unlike a read,
	// a cas result is a claim about the register's exact value at an instant.
	m := CASRegister{}
	s, _ := m.Step(m.Init(), write(1, history.OK))

	if _, ok := m.Step(s, cas(5, 2, true, history.OK)); ok {
		t.Fatal("a cas claiming it swapped from 5 must be rejected when the register holds 1")
	}
	if _, ok := m.Step(s, cas(1, 2, false, history.OK)); ok {
		t.Fatal("a cas claiming it did not swap must be rejected when the register does hold 1")
	}
}

func TestIndeterminateOperationsConstrainNothingButStillApply(t *testing.T) {
	m := CASRegister{}

	// An indeterminate read tells us nothing at all, whatever it claims to
	// have observed, and must leave the state untouched.
	next, ok := m.Step(3, read(999, history.Info))
	if !ok || next != 3 {
		t.Fatalf("an indeterminate read gave %v/%v, want 3/true", next, ok)
	}

	// An indeterminate write still takes effect where it is placed - that is
	// exactly why it cannot simply be deleted from the history.
	next, ok = m.Step(3, write(8, history.Info))
	if !ok || next != 8 {
		t.Fatalf("an indeterminate write gave %v/%v, want 8/true", next, ok)
	}

	// An indeterminate cas has no reported result, but its effect is still a
	// function of the state, so the transition is determined.
	next, ok = m.Step(1, cas(1, 2, false, history.Info))
	if !ok || next != 2 {
		t.Fatalf("an indeterminate cas against a matching value must swap; got %v/%v", next, ok)
	}
	next, ok = m.Step(9, cas(1, 2, true, history.Info))
	if !ok || next != 9 {
		t.Fatalf("an indeterminate cas against a non-matching value must not swap; got %v/%v", next, ok)
	}
}

func TestRegisterRefusesCAS(t *testing.T) {
	// Quietly ignoring an operation the model does not describe would report a
	// verdict about a system nobody was testing.
	m := Register{}
	if _, ok := m.Step(m.Init(), cas(0, 1, true, history.OK)); ok {
		t.Fatal("the plain register model accepted a compare-and-swap")
	}
	if _, ok := m.Step(m.Init(), cas(0, 1, true, history.Info)); ok {
		t.Fatal("the plain register model accepted an indeterminate compare-and-swap")
	}
}

func TestRegisterMatchesCASRegisterOnReadsAndWrites(t *testing.T) {
	plain, full := Register{}, CASRegister{}
	ops := []history.Op{
		read(0, history.OK), write(4, history.OK), read(4, history.OK),
		read(5, history.OK), write(1, history.Info), read(7, history.Info),
	}
	for _, op := range ops {
		for _, s := range []State{0, 4, 7} {
			p, pok := plain.Step(s, op)
			f, fok := full.Step(s, op)
			if p != f || pok != fok {
				t.Fatalf("models disagree on %v from %v: plain %v/%v, cas %v/%v", op.Kind, s, p, pok, f, fok)
			}
		}
	}
}

func TestStepRejectsAnUnknownKind(t *testing.T) {
	m := CASRegister{}
	bogus := history.Op{Key: "x", Kind: history.Kind(200), Outcome: history.OK}
	if _, ok := m.Step(m.Init(), bogus); ok {
		t.Fatal("an unrecognised operation kind must be rejected, not treated as a no-op")
	}
}

func TestStepRejectsAStateItDidNotProduce(t *testing.T) {
	m := CASRegister{}
	if _, ok := m.Step("not an int", read(0, history.OK)); ok {
		t.Fatal("the model accepted a state of the wrong type")
	}
}

func TestByName(t *testing.T) {
	for _, want := range All() {
		got, err := ByName(want.Name())
		if err != nil {
			t.Fatalf("ByName(%q): %v", want.Name(), err)
		}
		if got.Name() != want.Name() {
			t.Fatalf("ByName(%q) returned %q", want.Name(), got.Name())
		}
	}
	if _, err := ByName("counter"); err == nil {
		t.Fatal("ByName accepted a model that does not exist")
	}
}

func TestExplainSaysSomethingUseful(t *testing.T) {
	// A counterexample nobody can read is not a counterexample.
	m := CASRegister{}
	for _, op := range []history.Op{
		read(3, history.OK),
		cas(5, 2, true, history.OK),
		cas(1, 2, false, history.OK),
	} {
		if got := m.Explain(1, op); len(got) < 20 {
			t.Errorf("Explain(%v) = %q, which is too terse to help", op.Kind, got)
		}
	}
	if got := (Register{}).Explain(1, cas(0, 1, true, history.OK)); got == "" {
		t.Error("the register model must explain that it does not describe cas")
	}
}

func TestDescribe(t *testing.T) {
	if got := (CASRegister{}).Describe(7); got != "7" {
		t.Fatalf("Describe(7) = %q, want \"7\"", got)
	}
}
