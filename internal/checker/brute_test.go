package checker

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

func TestBruteForceRefusesOversizedHistories(t *testing.T) {
	h := cleanTraffic(0, 0, 6) // 13 operations
	got, err := BruteForce(h, casReg, 8)
	if err == nil {
		t.Fatalf("BruteForce accepted %d operations with a limit of 8", len(h))
	}
	if got == Linearizable {
		t.Error("the refusal came back as linearizable; it must fail closed")
	}
}

func TestBruteForceRejectsANilModel(t *testing.T) {
	if _, err := BruteForce(history.History{rd(0, 0, 0, 1)}, nil, 8); err == nil {
		t.Fatal("BruteForce accepted a nil model")
	}
}

// TestBruteForceMatchesTheCorpus runs the reference checker over the same
// hand-argued histories as the fast one. If these two disagree here, the
// differential test below is checking two broken things against each other.
func TestBruteForceMatchesTheCorpus(t *testing.T) {
	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			got, err := BruteForce(c.ops, c.model, 8)
			if err != nil {
				t.Fatalf("BruteForce: %v", err)
			}
			if got != c.want {
				t.Fatalf("verdict = %v, want %v (%s)", got, c.want, c.why)
			}
		})
	}
}

func TestBruteForceDropsFailedOperations(t *testing.T) {
	failed := history.Op{
		Process: 0, Key: "x", Kind: history.Write, Value: 1,
		Outcome: history.Fail, Invoke: 0, Complete: 10, Err: "connection refused",
	}
	got, err := BruteForce(history.History{failed, rd(1, 1, 20, 30)}, casReg, 8)
	if err != nil {
		t.Fatalf("BruteForce: %v", err)
	}
	if got != NotLinearizable {
		t.Errorf("verdict = %v, want not linearizable: a Fail write must not explain a read of 1", got)
	}
}

// genValues is the size of the value domain the generator draws from. Three is
// enough to make stale reads distinguishable from fresh ones and small enough
// that random outputs still land on legal values reasonably often.
const genValues = 3

// TestDifferentialAgainstBruteForce is the strongest evidence in this package:
// several thousand random small histories, checked twice by two programs that
// share no search code.
func TestDifferentialAgainstBruteForce(t *testing.T) {
	const runs = 20000
	rng := rand.New(rand.NewPCG(0x5b17b7a10f1e2d3c, 0xc0ffee1234567890))

	// Every history is checked against both models. The plain register rejects
	// compare-and-swap outright, which exercises a failure that has nothing to
	// do with what a client observed, and both checkers have to agree there too.
	models := []model.Model{casReg, model.Register{}}

	var lin, notLin, plainDisagreeChecked int
	for i := 0; i < runs; i++ {
		h := randomHistory(t, rng)
		if err := h.Validate(); err != nil {
			t.Fatalf("case %d: the generator produced an invalid history: %v\n%s", i, err, dump(h))
		}

		for _, m := range models {
			want, err := BruteForce(h, m, 8)
			if err != nil {
				t.Fatalf("case %d (%s): BruteForce: %v", i, m.Name(), err)
			}
			got, err := Check(h, m, Options{})
			if err != nil {
				t.Fatalf("case %d (%s): Check: %v", i, m.Name(), err)
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
			} else {
				plainDisagreeChecked++
			}
		}
	}

	t.Logf("differential corpus: %d histories against %d models, %d linearizable, %d not linearizable "+
		"(plus %d plain-register cross-checks)", runs, len(models), lin, notLin, plainDisagreeChecked)
	const floor = runs * 15 / 100
	if lin < floor || notLin < floor {
		t.Fatalf("verdict mix is too lopsided (%d linearizable, %d not, floor %d each): a "+
			"differential test in which one verdict barely occurs proves almost nothing", lin, notLin, floor)
	}
}

// TestDifferentialMinimisationReproduces checks the other half of minimisation
// over the same random corpus: whatever truncation the checker hands back must
// itself fail, according to the reference checker rather than to itself.
func TestDifferentialMinimisationReproduces(t *testing.T) {
	const runs = 600
	rng := rand.New(rand.NewPCG(0x9e3779b97f4a7c15, 0x2545f4914f6cdd1d))

	checked := 0
	for i := 0; i < runs; i++ {
		h := randomHistory(t, rng)
		got, err := Check(h, casReg, Options{Minimize: true})
		if err != nil {
			t.Fatalf("case %d: Check: %v", i, err)
		}
		if got.Verdict != NotLinearizable {
			continue
		}
		if len(got.Ops) == 0 {
			t.Fatalf("case %d: Minimize was set but no truncation came back\n%s", i, dump(h))
		}
		if len(got.Ops) > len(h.DropFailed()) {
			t.Fatalf("case %d: the truncation is larger than the history", i)
		}
		ref, err := BruteForce(got.Ops, casReg, 8)
		if err != nil {
			t.Fatalf("case %d: BruteForce on the truncation: %v", i, err)
		}
		if ref != NotLinearizable {
			t.Fatalf("case %d: the reported truncation is %v by the reference checker\noriginal:\n%struncation:\n%s",
				i, ref, dump(h), dump(got.Ops))
		}
		if got.Culprit == nil {
			t.Fatalf("case %d: a truncation with no culprit", i)
		}
		checked++
	}
	t.Logf("minimisation reproduced on %d of %d random histories", checked, runs)
	if checked < runs/10 {
		t.Fatalf("only %d of %d histories failed, so this test barely exercised minimisation", checked, runs)
	}
}

// randomHistory builds one small single-key history that history.Validate will
// accept: processes are sequential, and an indeterminate operation is always
// the last one its process issued.
//
// Outputs are filled in by simulating a real linearization, so the history
// starts out linearizable by construction; roughly half are then corrupted by
// altering one reported output. A corrupted history is often but not always a
// violation, which is what gives the differential test both verdicts.
func randomHistory(tb testing.TB, rng *rand.Rand) history.History {
	tb.Helper()

	nProc := 2 + rng.IntN(2)
	total := 1 + rng.IntN(8) // up to the largest history BruteForce will accept

	cursors := make([]int64, nProc)
	closed := make([]bool, nProc)
	for i := range cursors {
		cursors[i] = int64(rng.IntN(6))
	}

	ops := make(history.History, 0, total)
	for len(ops) < total {
		var openProcs []int
		for p := range nProc {
			if !closed[p] {
				openProcs = append(openProcs, p)
			}
		}
		if len(openProcs) == 0 {
			break
		}
		p := openProcs[rng.IntN(len(openProcs))]

		inv := cursors[p] + int64(rng.IntN(4))
		comp := inv + 1 + int64(rng.IntN(6))
		op := history.Op{Process: p, Key: "x", Invoke: inv, Complete: comp, Outcome: history.OK}
		switch rng.IntN(3) {
		case 0:
			op.Kind = history.Read
		case 1:
			op.Kind = history.Write
			op.Value = rng.IntN(genValues)
		default:
			op.Kind = history.CAS
			op.From = rng.IntN(genValues)
			// history.Validate rejects a cas from a value to itself.
			op.To = (op.From + 1 + rng.IntN(genValues-1)) % genValues
		}
		if rng.IntN(100) < 18 {
			op.Outcome = history.Info
			op.Complete = history.Pending
			closed[p] = true
		}
		cursors[p] = comp + int64(rng.IntN(3))
		ops = append(ops, op)
	}

	fillOutputs(tb, ops, rng)
	if rng.IntN(100) < 55 {
		corruptOneOutput(ops, rng)
	}
	return ops
}

// fillOutputs picks a linearization point inside each operation's interval,
// runs the model in that order, and records what each client would have seen.
//
// Sorting by linearization point cannot contradict real time: if A completed
// before B was invoked then A's point is strictly below A's completion, which
// is at most B's invocation, which is at most B's point. Equal points therefore
// only happen between operations that overlap, where either order is legal.
func fillOutputs(tb testing.TB, ops history.History, rng *rand.Rand) {
	tb.Helper()
	if len(ops) == 0 {
		return
	}

	var maxT int64
	for _, op := range ops {
		if op.Invoke > maxT {
			maxT = op.Invoke
		}
		if op.Complete != history.Pending && op.Complete > maxT {
			maxT = op.Complete
		}
	}

	pts := make([]int64, len(ops))
	for i, op := range ops {
		if op.Outcome == history.Info {
			// A pending operation may linearize at any time from its
			// invocation onwards, including after everything else.
			pts[i] = op.Invoke + int64(rng.IntN(int(maxT-op.Invoke)+2))
		} else {
			pts[i] = op.Invoke + int64(rng.IntN(int(op.Complete-op.Invoke)))
		}
	}

	order := make([]int, len(ops))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return pts[order[a]] < pts[order[b]] })

	s := model.State(model.InitialValue)
	for _, i := range order {
		op := &ops[i]
		v := s.(int)
		switch op.Kind {
		case history.Read:
			op.Observed = v
		case history.CAS:
			op.Swapped = v == op.From
		}
		next, ok := casReg.Step(s, *op)
		if !ok {
			tb.Fatalf("the generator produced an output the model rejects: %+v against state %v", *op, s)
		}
		s = next
	}
}

// corruptOneOutput alters what one client claims to have seen. The history may
// still be linearizable afterwards - another order may explain the new output -
// which is exactly the interesting middle ground.
func corruptOneOutput(ops history.History, rng *rand.Rand) {
	var candidates []int
	for i, op := range ops {
		if op.Outcome != history.OK {
			continue
		}
		if op.Kind == history.Read || op.Kind == history.CAS {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return
	}
	op := &ops[candidates[rng.IntN(len(candidates))]]
	switch op.Kind {
	case history.Read:
		op.Observed = (op.Observed + 1 + rng.IntN(genValues-1)) % genValues
	case history.CAS:
		op.Swapped = !op.Swapped
	}
}

// dump renders a history in invocation order for a failure message.
func dump(h history.History) string {
	sorted := append(history.History{}, h...)
	sorted.SortByInvoke()

	var b strings.Builder
	for _, op := range sorted {
		fmt.Fprintf(&b, "  p%d %-5s %-4s", op.Process, op.Kind, op.Outcome)
		switch op.Kind {
		case history.Read:
			fmt.Fprintf(&b, " -> %d", op.Observed)
		case history.Write:
			fmt.Fprintf(&b, " %d", op.Value)
		case history.CAS:
			fmt.Fprintf(&b, " %d->%d swapped=%t", op.From, op.To, op.Swapped)
		}
		if op.Complete == history.Pending {
			fmt.Fprintf(&b, "  [%d, pending]\n", op.Invoke)
		} else {
			fmt.Fprintf(&b, "  [%d, %d]\n", op.Invoke, op.Complete)
		}
	}
	return b.String()
}
