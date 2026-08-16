package checker

import (
	"math/rand/v2"
	"testing"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// The generator in brute_test.go builds histories the way a working harness
// would: durations of at least one tick, timestamps spread out, outputs filled
// in by simulating a real linearization. That is the right shape for finding
// ordinary search bugs, and it is exactly the wrong shape for finding the ones
// that live in the degenerate corners, because it cannot reach them. It never
// produces two operations that share a timestamp, never produces a zero-width
// interval, and never produces an output no order could explain.
//
// This file generates the opposite. Time is squeezed into a handful of distinct
// values so exact collisions are the rule; intervals may be zero-width; values
// come from a domain of two, so a stale read and a fresh one are hard to tell
// apart; most operations may belong to one process, or most may be
// indeterminate. Outputs are drawn at random rather than simulated, so a
// history is only accidentally linearizable and the two checkers have to agree
// about garbage as well as about plausible runs.
//
// Everything here is measured against BruteForce, which is the reference.

// adversarialShape parameterises the generator below.
type adversarialShape struct {
	name string

	// zeroDuration is the percentage of operations whose invocation and
	// completion coincide.
	zeroDuration int
	// values is the size of the value domain.
	values int
	// info is the percentage of operations the client never heard back about.
	info int
	// oneProcess is the percentage of histories placed entirely on one
	// process.
	oneProcess int
	// spread bounds each time step, so a small spread packs operations onto a
	// few distinct instants.
	spread int
}

var adversarialShapes = []adversarialShape{
	{name: "zero_width_intervals", zeroDuration: 45, values: 2, info: 10, oneProcess: 40, spread: 3},
	{name: "colliding_timestamps", zeroDuration: 0, values: 2, info: 10, oneProcess: 40, spread: 3},
	{name: "mostly_indeterminate", zeroDuration: 0, values: 2, info: 55, oneProcess: 20, spread: 4},
	{name: "almost_one_process", zeroDuration: 0, values: 2, info: 5, oneProcess: 95, spread: 2},
	// This one is about the value domain and a wider time spread, not about
	// zero-width intervals, so it does not ask for them and is not held to the
	// coincidence floor below. A few still occur, because a step of zero is a
	// legal draw.
	{name: "two_value_domain", zeroDuration: 0, values: 2, info: 20, oneProcess: 10, spread: 9},
}

// adversarialHistory builds one small single-key history in the shape above. It
// may be one history.Validate refuses; callers are expected to handle that,
// because whether a malformed history is refused is itself under test.
func adversarialHistory(rng *rand.Rand, s adversarialShape) history.History {
	nProc := 1 + rng.IntN(3)
	if rng.IntN(100) < s.oneProcess {
		nProc = 1
	}
	total := 1 + rng.IntN(8) // never more than BruteForce will accept

	cursors := make([]int64, nProc)
	retired := make([]bool, nProc)

	ops := make(history.History, 0, total)
	for len(ops) < total {
		var open []int
		for p := range nProc {
			if !retired[p] {
				open = append(open, p)
			}
		}
		if len(open) == 0 {
			break
		}
		p := open[rng.IntN(len(open))]

		invoke := cursors[p] + int64(rng.IntN(s.spread))
		complete := invoke
		if rng.IntN(100) >= s.zeroDuration {
			complete = invoke + int64(rng.IntN(s.spread))
		}

		op := history.Op{Process: p, Key: "x", Invoke: invoke, Complete: complete, Outcome: history.OK}
		switch rng.IntN(3) {
		case 0:
			op.Kind = history.Read
			op.Observed = rng.IntN(s.values)
		case 1:
			op.Kind = history.Write
			op.Value = rng.IntN(s.values)
		default:
			op.Kind = history.CAS
			op.From = rng.IntN(s.values)
			op.To = (op.From + 1 + rng.IntN(s.values-1)) % s.values
			op.Swapped = rng.IntN(2) == 0
		}

		if rng.IntN(100) < s.info {
			// A client with no answer cannot issue anything else.
			op.Outcome = history.Info
			op.Complete = history.Pending
			retired[p] = true
			cursors[p] = invoke
		} else {
			cursors[p] = complete
		}
		ops = append(ops, op)
	}
	return ops
}

// coincidentInstants counts the operations that share a (key, instant) with
// another zero-width operation. It is the degenerate class this file exists to
// reach, so the tests below assert it is reached in quantity rather than
// hoping.
func coincidentInstants(h history.History) int {
	type at struct {
		key string
		t   int64
	}
	seen := map[at]int{}
	for _, op := range h {
		if op.Outcome != history.Info && op.Complete == op.Invoke {
			seen[at{op.Key, op.Invoke}]++
		}
	}
	n := 0
	for _, c := range seen {
		if c > 1 {
			n += c
		}
	}
	return n
}

const adversarialRuns = 20000

// TestAdversarialDifferentialAgainstBruteForce runs the degenerate corpus
// through both checkers against both models.
//
// A history the validator refuses is not skipped quietly. The refusal is the
// interesting half: Check has to report the error AND a verdict that is not a
// pass, because a caller that ignores the error must not read one.
func TestAdversarialDifferentialAgainstBruteForce(t *testing.T) {
	models := []model.Model{casReg, model.Register{}}

	for _, shape := range adversarialShapes {
		t.Run(shape.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0xdeadbeefcafe, 0x1234abcd5678))
			var valid, refused, coincident, lin, notLin, withInfo int

			for i := 0; i < adversarialRuns; i++ {
				h := adversarialHistory(rng, shape)
				if coincidentInstants(h) > 0 {
					coincident++
				}
				if _, _, info := h.Counts(); info > 0 {
					withInfo++
				}

				if err := h.Validate(); err != nil {
					refused++
					got, cerr := Check(h, casReg, Options{})
					if cerr == nil {
						t.Fatalf("case %d: Validate refuses this history but Check accepted it\n%s\n%v", i, dump(h), err)
					}
					if got.Verdict == Linearizable {
						t.Fatalf("case %d: Check returned an error and a verdict of %v; a malformed "+
							"history must never read as a pass\n%s", i, got.Verdict, dump(h))
					}
					continue
				}
				valid++

				for _, m := range models {
					want, err := BruteForce(h, m, 8)
					if err != nil {
						t.Fatalf("case %d (%s): BruteForce: %v", i, m.Name(), err)
					}
					got, err := Check(h, m, Options{})
					if err != nil {
						t.Fatalf("case %d (%s): Check: %v\n%s", i, m.Name(), err, dump(h))
					}
					if got.Verdict == Unknown {
						t.Fatalf("case %d (%s): Check returned unknown with no budget set\n%s", i, m.Name(), dump(h))
					}
					if got.Verdict != want {
						t.Fatalf("case %d (%s): Check says %v, BruteForce says %v\nreason: %s\n%s",
							i, m.Name(), got.Verdict, want, got.Reason, dump(h))
					}
					if m == models[0] {
						switch want {
						case Linearizable:
							lin++
						case NotLinearizable:
							notLin++
						}
					}
				}
			}

			t.Logf("%s: %d histories, %d valid, %d refused by the validator, %d containing coincident "+
				"zero-width operations, %d with indeterminate operations, %d linearizable, %d not",
				shape.name, adversarialRuns, valid, refused, coincident, withInfo, lin, notLin)

			// A differential test proves nothing if the corpus is lopsided or if
			// the degenerate cases it was written for never occur.
			const floor = adversarialRuns / 20
			if lin < floor || notLin < floor {
				t.Errorf("verdict mix is too lopsided (%d linearizable, %d not, floor %d each)", lin, notLin, floor)
			}
			if withInfo < floor {
				t.Errorf("only %d histories carried an indeterminate operation, floor %d", withInfo, floor)
			}
			if shape.zeroDuration > 0 && coincident < floor {
				t.Errorf("only %d histories had coincident zero-width operations, floor %d; this shape "+
					"exists to produce them", coincident, floor)
			}
		})
	}
}

// TestTwoZeroWidthOperationsAtOneInstantAreNotAPass pins the single class of
// input on which the search cannot agree with its own oracle.
//
// Both operations begin and end at the same instant, so by the rule the rest of
// this tool uses - A precedes B when A completes at or before B is invoked -
// each of them strictly precedes the other. Real time orders them both ways, no
// linearization can respect that, and BruteForce duly refuses to place either.
// The fast search cannot express the contradiction: its entry list is linear,
// so one of the two has to come first, and left to itself it picks an order and
// reports a pass. Which order it picks depends on where the operations sit in
// the input slice, which history says carries no meaning.
//
// clock.Completion is what stops a run producing a zero-width interval. This is
// what stops a history file supplying one.
func TestTwoZeroWidthOperationsAtOneInstantAreNotAPass(t *testing.T) {
	cases := []struct {
		name string
		ops  history.History
	}{
		{"write_then_read_one_process", history.History{wr(0, 1, 10, 10), rd(0, 1, 10, 10)}},
		{"read_then_write_one_process", history.History{rd(0, 1, 10, 10), wr(0, 1, 10, 10)}},
		{"write_then_read_two_processes", history.History{wr(0, 1, 10, 10), rd(1, 1, 10, 10)}},
		{"cas_and_write", history.History{wr(0, 1, 7, 7), cas(1, 0, 1, true, 7, 7)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ref, err := BruteForce(c.ops, casReg, 8)
			if err != nil {
				t.Fatalf("BruteForce: %v", err)
			}
			if ref != NotLinearizable {
				t.Fatalf("the reference checker says %v; this test is built on it saying otherwise", ref)
			}

			got, cerr := Check(c.ops, casReg, Options{})
			if got.Verdict == Linearizable {
				t.Errorf("Check passed a history the reference checker refuses: two zero-width "+
					"operations at instant 10 each precede the other, so no order respects real time "+
					"(error: %v)", cerr)
			}
		})
	}

	// One zero-width operation on its own is fine and must stay so: it orders
	// consistently against everything around it.
	lone := history.History{wr(0, 1, 0, 10), rd(1, 1, 20, 20), rd(2, 1, 30, 40)}
	got, err := Check(lone, casReg, Options{})
	if err != nil {
		t.Fatalf("a single zero-width operation was refused: %v", err)
	}
	if got.Verdict != Linearizable {
		t.Errorf("verdict = %v, want linearizable; a lone instantaneous operation is not degenerate", got.Verdict)
	}
}

// TestAdversarialMinimisationReproduces checks, over the degenerate corpus,
// that every counterexample the checker hands back is itself a history: it
// validates, and it still fails according to the reference checker rather than
// according to the search that produced it.
func TestAdversarialMinimisationReproduces(t *testing.T) {
	for _, shape := range adversarialShapes {
		t.Run(shape.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0xfeedfacefeed, 0x0badc0de))
			checked := 0
			for i := 0; i < adversarialRuns; i++ {
				h := adversarialHistory(rng, shape)
				if err := h.Validate(); err != nil {
					continue
				}
				got, err := Check(h, casReg, Options{Minimize: true})
				if err != nil {
					t.Fatalf("case %d: Check: %v", i, err)
				}
				if got.Verdict != NotLinearizable {
					continue
				}
				if len(got.Ops) == 0 {
					t.Fatalf("case %d: a violation with no truncation\n%s", i, dump(h))
				}
				if err := got.Ops.Validate(); err != nil {
					t.Fatalf("case %d: the truncation is not a valid history: %v\noriginal:\n%struncation:\n%s",
						i, err, dump(h), dump(got.Ops))
				}
				ref, err := BruteForce(got.Ops, casReg, 64)
				if err != nil {
					t.Fatalf("case %d: BruteForce on the truncation: %v", i, err)
				}
				if ref != NotLinearizable {
					t.Fatalf("case %d: the truncation checks as %v by the reference checker, so it does "+
						"not reproduce\noriginal:\n%struncation:\n%s", i, ref, dump(h), dump(got.Ops))
				}
				checked++
			}
			t.Logf("%s: %d counterexamples validated and reproduced", shape.name, checked)
			if checked < adversarialRuns/20 {
				t.Errorf("only %d counterexamples were produced, too few for this to mean anything", checked)
			}
		})
	}
}
