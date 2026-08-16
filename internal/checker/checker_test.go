package checker

import (
	"strings"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// The helpers below build operations on key "x" unless retagged by onKey. They
// keep the corpus readable: a test whose intent is buried in struct literals is
// a test nobody re-reads when it starts failing.

func rd(proc, observed int, inv, comp int64) history.Op {
	return history.Op{
		Process: proc, Key: "x", Kind: history.Read,
		Observed: observed, Outcome: history.OK,
		Invoke: inv, Complete: comp,
	}
}

func wr(proc, value int, inv, comp int64) history.Op {
	return history.Op{
		Process: proc, Key: "x", Kind: history.Write,
		Value: value, Outcome: history.OK,
		Invoke: inv, Complete: comp,
	}
}

func cas(proc, from, to int, swapped bool, inv, comp int64) history.Op {
	return history.Op{
		Process: proc, Key: "x", Kind: history.CAS,
		From: from, To: to, Swapped: swapped, Outcome: history.OK,
		Invoke: inv, Complete: comp,
	}
}

// infoWr and infoCAS are operations the client never got an answer for. They
// carry no completion time, which is what history.Validate insists on.
func infoWr(proc, value int, inv int64) history.Op {
	return history.Op{
		Process: proc, Key: "x", Kind: history.Write,
		Value: value, Outcome: history.Info,
		Invoke: inv, Complete: history.Pending,
	}
}

func infoCAS(proc, from, to int, inv int64) history.Op {
	return history.Op{
		Process: proc, Key: "x", Kind: history.CAS,
		From: from, To: to, Outcome: history.Info,
		Invoke: inv, Complete: history.Pending,
	}
}

// onKey retags a group of operations onto another key and shifts their process
// numbers, so several independent groups can be spliced into one history
// without two operations of the same process overlapping in time.
func onKey(k string, procBase int, ops ...history.Op) history.History {
	out := make(history.History, len(ops))
	for i, op := range ops {
		op.Key = k
		op.Process += procBase
		out[i] = op
	}
	return out
}

// corpusCase is a hand-written history whose verdict is known by argument, not
// by running the checker. why records that argument.
type corpusCase struct {
	name  string
	why   string
	ops   history.History
	model model.Model
	want  Verdict
}

var casReg = model.CASRegister{}

// corpus is shared with the brute-force reference test in brute_test.go: the
// two checkers must agree on every case, and both must agree with the argument
// written down in why.
var corpus = []corpusCase{
	{
		name:  "empty_history",
		why:   "nothing to order, so the empty order works",
		ops:   history.History{},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "lone_read_of_the_initial_value",
		why:   "registers start at model.InitialValue, so reading 0 first is fine",
		ops:   history.History{rd(0, 0, 0, 10)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "lone_read_of_a_value_nobody_wrote",
		why:   "the register held 0 and nothing wrote 1, so no order explains the read",
		ops:   history.History{rd(0, 1, 0, 10)},
		model: casReg,
		want:  NotLinearizable,
	},
	{
		name:  "sequential_write_then_read_of_that_value",
		why:   "the only possible order is write then read, and it fits",
		ops:   history.History{wr(0, 1, 0, 10), rd(0, 1, 10, 20)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "sequential_write_then_stale_read",
		why:   "the write completed before the read was invoked, so 0 is unreachable",
		ops:   history.History{wr(0, 1, 0, 10), rd(0, 0, 10, 20)},
		model: casReg,
		want:  NotLinearizable,
	},
	{
		name:  "stale_read_concurrent_with_the_write",
		why:   "the read overlaps the write and may linearize before it",
		ops:   history.History{wr(0, 1, 0, 10), rd(1, 0, 2, 4)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "fresh_read_concurrent_with_the_write",
		why:   "the same overlap lets the write linearize first instead",
		ops:   history.History{wr(0, 1, 0, 10), rd(1, 1, 2, 4)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name: "classic_violation_fresh_read_then_stale_read",
		why: "once one client has observed 1, a strictly later read cannot see 0: " +
			"the write must precede the first read, and nothing writes 0 back",
		ops:   history.History{wr(0, 1, 0, 10), rd(1, 1, 2, 4), rd(2, 0, 5, 8)},
		model: casReg,
		want:  NotLinearizable,
	},
	{
		name:  "two_fresh_reads_during_one_write",
		why:   "the write linearizes before both reads, which is consistent",
		ops:   history.History{wr(0, 1, 0, 10), rd(1, 1, 2, 4), rd(2, 1, 5, 8)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name: "stale_read_then_fresh_read_during_one_long_write",
		why: "a stale read is legal while it is concurrent with the write: order " +
			"read 0, then the write, then read 1",
		ops:   history.History{wr(0, 1, 0, 100), rd(1, 0, 10, 20), rd(2, 1, 30, 40)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "indeterminate_write_that_did_take_effect",
		why:   "the pending write may be placed before the read, explaining the 1",
		ops:   history.History{infoWr(0, 1, 0), rd(1, 1, 10, 20)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "indeterminate_write_that_did_not_take_effect",
		why:   "the pending write may instead be placed after the read, explaining the 0",
		ops:   history.History{infoWr(0, 1, 0), rd(1, 0, 10, 20)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "cas_that_tells_the_truth",
		why:   "the register holds 1 when the cas runs, so swapping to 2 is honest",
		ops:   history.History{wr(0, 1, 0, 10), cas(0, 1, 2, true, 10, 20), rd(0, 2, 20, 30)},
		model: casReg,
		want:  Linearizable,
	},
	{
		name:  "cas_claiming_a_swap_it_could_not_have_made",
		why:   "the register held 1, so a cas expecting 5 cannot have swapped",
		ops:   history.History{wr(0, 1, 0, 10), cas(0, 5, 2, true, 10, 20)},
		model: casReg,
		want:  NotLinearizable,
	},
	{
		name:  "cas_denying_a_swap_it_must_have_made",
		why:   "the register held exactly 1, so the cas was obliged to swap",
		ops:   history.History{wr(0, 1, 0, 10), cas(0, 1, 2, false, 10, 20)},
		model: casReg,
		want:  NotLinearizable,
	},
	{
		name: "split_brain_signature",
		why: "after both writes complete the register holds 1 or 2 and stays there; " +
			"two sequential reads seeing 2 then 1 need a write between them",
		ops: history.History{
			wr(0, 1, 0, 10), wr(1, 2, 0, 10),
			rd(2, 2, 20, 25), rd(2, 1, 26, 30),
		},
		model: casReg,
		want:  NotLinearizable,
	},
	{
		name:  "register_model_refuses_a_history_containing_cas",
		why:   "the plain register does not describe compare-and-swap and must not wave it through",
		ops:   history.History{wr(0, 1, 0, 10), cas(0, 1, 2, true, 10, 20)},
		model: model.Register{},
		want:  NotLinearizable,
	},
	{
		name:  "zero_duration_operation",
		why:   "an operation whose invoke and complete coincide still has to be placed after its own call",
		ops:   history.History{rd(0, 0, 5, 5)},
		model: casReg,
		want:  Linearizable,
	},
}

func TestCorpus(t *testing.T) {
	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			got, err := Check(c.ops, c.model, Options{})
			if err != nil {
				t.Fatalf("Check returned an error: %v", err)
			}
			if got.Verdict != c.want {
				t.Fatalf("verdict = %v, want %v (%s)\nreason: %s", got.Verdict, c.want, c.why, got.Reason)
			}
			if got.Verdict == NotLinearizable && got.Reason == "" {
				t.Error("a violation was reported with no reason")
			}
		})
	}
}

// TestIndeterminateOperationCannotBeIgnored pins both halves of the rule that
// Info operations stay in the history.
//
// The first half is the easy one. The second needed an argument, and the
// argument changed the test: with these register models an indeterminate
// operation can ALWAYS be placed last, where nothing observes it, so it can
// never on its own turn a linearizable history into an unlinearizable one.
// The only way discarding one can flip a verdict from "not linearizable" to
// "linearizable" is when the model rejects the operation itself, which is
// exactly what the plain register does to a compare-and-swap. That is the case
// pinned below.
func TestIndeterminateOperationCannotBeIgnored(t *testing.T) {
	t.Run("linearizable_only_because_the_pending_cas_may_have_happened", func(t *testing.T) {
		withInfo := history.History{infoCAS(0, 0, 5, 0), rd(1, 5, 10, 20)}
		withoutInfo := history.History{rd(1, 5, 10, 20)}

		got, err := Check(withInfo, casReg, Options{})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if got.Verdict != Linearizable {
			t.Errorf("with the pending cas: verdict = %v, want linearizable (reason: %s)", got.Verdict, got.Reason)
		}

		// If this were linearizable too, the case above would prove nothing.
		got, err = Check(withoutInfo, casReg, Options{})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if got.Verdict != NotLinearizable {
			t.Errorf("without the pending cas: verdict = %v, want not linearizable", got.Verdict)
		}
	})

	t.Run("not_linearizable_and_would_be_accepted_if_info_ops_were_deleted", func(t *testing.T) {
		withInfo := history.History{wr(0, 1, 0, 10), infoCAS(1, 1, 2, 20)}
		withoutInfo := history.History{wr(0, 1, 0, 10)}

		got, err := Check(withInfo, model.Register{}, Options{})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if got.Verdict != NotLinearizable {
			t.Errorf("with the pending cas: verdict = %v, want not linearizable", got.Verdict)
		}

		got, err = Check(withoutInfo, model.Register{}, Options{})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if got.Verdict != Linearizable {
			t.Errorf("without the pending cas: verdict = %v, want linearizable; "+
				"if this is not linearizable the test is not pinning the deletion", got.Verdict)
		}
	})
}

// TestMultiKeyReportsTheFailingKey checks the compositional split: two clean
// keys and one carrying the classic violation.
func TestMultiKeyReportsTheFailingKey(t *testing.T) {
	var h history.History
	h = append(h, onKey("alpha", 10, wr(0, 7, 0, 10), rd(1, 7, 20, 30))...)
	h = append(h, onKey("bravo", 20, wr(0, 1, 0, 10), rd(1, 1, 2, 4), rd(2, 0, 5, 8))...)
	h = append(h, onKey("charlie", 30, wr(0, 3, 0, 10), rd(1, 0, 2, 4))...)

	got, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != NotLinearizable {
		t.Fatalf("verdict = %v, want not linearizable", got.Verdict)
	}
	if got.Key != "bravo" {
		t.Errorf("Key = %q, want %q", got.Key, "bravo")
	}
	for _, clean := range []string{"alpha", "charlie"} {
		st, ok := got.PerKey[clean]
		if !ok {
			t.Errorf("PerKey has no entry for %q", clean)
			continue
		}
		if st.Verdict != Linearizable {
			t.Errorf("PerKey[%q].Verdict = %v, want linearizable", clean, st.Verdict)
		}
	}
	if st := got.PerKey["bravo"]; st.Verdict != NotLinearizable {
		t.Errorf("PerKey[%q].Verdict = %v, want not linearizable", "bravo", st.Verdict)
	}
	if st := got.PerKey["bravo"]; st.Ops != 3 {
		t.Errorf("PerKey[%q].Ops = %d, want 3", "bravo", st.Ops)
	}
	if !strings.Contains(got.Reason, "bravo") {
		t.Errorf("Reason = %q, want it to name the failing key", got.Reason)
	}
}

// TestRegisterModelExplainsItselfOnCAS checks that the refusal in the corpus
// comes with the advice the model wrote, not a bare "not linearizable".
func TestRegisterModelExplainsItselfOnCAS(t *testing.T) {
	h := history.History{wr(0, 1, 0, 10), cas(0, 1, 2, true, 10, 20)}
	got, err := Check(h, model.Register{}, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != NotLinearizable {
		t.Fatalf("verdict = %v, want not linearizable", got.Verdict)
	}
	if !strings.Contains(got.Reason, "cas-register") {
		t.Errorf("Reason = %q, want it to point at the cas-register model", got.Reason)
	}
}

// budgetHistory is six mutually concurrent operations that do linearize, but
// only after the search has backtracked: w1 r1 w2 r2 w3 r3.
func budgetHistory() history.History {
	return history.History{
		wr(0, 1, 0, 100), wr(1, 2, 0, 100), wr(2, 3, 0, 100),
		rd(3, 1, 0, 100), rd(4, 2, 0, 100), rd(5, 3, 0, 100),
	}
}

func TestBudgetExhaustionIsUnknownAndNeverLinearizable(t *testing.T) {
	h := budgetHistory()

	full, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if full.Verdict != Linearizable {
		t.Fatalf("with no budget: verdict = %v, want linearizable (reason: %s)", full.Verdict, full.Reason)
	}
	if full.Visits <= 3 {
		t.Fatalf("the unbudgeted search took %d visits; the budget below would not bite", full.Visits)
	}

	got, err := Check(h, casReg, Options{MaxVisits: 3})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != Unknown {
		t.Errorf("with MaxVisits=3: verdict = %v, want unknown", got.Verdict)
	}
	if got.Verdict == Linearizable {
		t.Error("an exhausted search was reported as linearizable; this tool must fail closed")
	}
	if got.Key != "x" {
		t.Errorf("Key = %q, want the key that ran out of budget", got.Key)
	}
	if !strings.Contains(got.Reason, "budget") {
		t.Errorf("Reason = %q, want it to say the budget ran out", got.Reason)
	}
}

// exhaustiveWorkload is a history the search cannot decide cheaply: pairs of
// mutually concurrent writes and reads, plus one read of a value nobody ever
// wrote, so no linearization exists and the search has to exhaust the space.
func exhaustiveWorkload(pairs int) history.History {
	var h history.History
	proc := 0
	for i := 0; i < pairs; i++ {
		h = append(h, wr(proc, i+1, 0, 1000))
		proc++
		h = append(h, rd(proc, i+1, 0, 1000))
		proc++
	}
	h = append(h, rd(proc, 999, 0, 1000))
	return h
}

// TestTimeoutIsUnknown needs a workload measurably slower than the clock.
//
// A one-nanosecond timeout looks like the obvious test and does not work: the
// monotonic clock on the development machine has roughly millisecond
// granularity, so time.Now() called twice in a row reports no elapsed time at
// all and a sub-millisecond deadline is simply never observed to pass. The
// test therefore states its own precondition - the unbudgeted search must take
// far longer than the timeout - so that it fails loudly rather than quietly
// becoming vacuous on faster hardware.
func TestTimeoutIsUnknown(t *testing.T) {
	const timeout = time.Millisecond
	h := exhaustiveWorkload(9)

	full, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if full.Verdict != NotLinearizable {
		t.Fatalf("unbudgeted verdict = %v, want not linearizable", full.Verdict)
	}
	if full.Elapsed < 10*timeout {
		t.Fatalf("the unbudgeted search took %v, too close to the %v timeout for this "+
			"test to mean anything; make the workload larger", full.Elapsed, timeout)
	}

	got, err := Check(h, casReg, Options{Timeout: timeout})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != Unknown {
		t.Fatalf("verdict = %v, want unknown (reason: %s)", got.Verdict, got.Reason)
	}
	if !strings.Contains(got.Reason, "budget") {
		t.Errorf("Reason = %q, want it to say the budget ran out", got.Reason)
	}
	if got.Elapsed <= 0 {
		t.Errorf("Elapsed = %v, want a positive duration on a workload this size", got.Elapsed)
	}
}

func TestCheckRejectsMalformedHistories(t *testing.T) {
	cases := []struct {
		name string
		ops  history.History
	}{
		{"negative_invoke_time", history.History{{Process: 0, Key: "x", Kind: history.Read, Outcome: history.OK, Invoke: -1, Complete: 1}}},
		{"empty_key", history.History{{Process: 0, Key: "", Kind: history.Read, Outcome: history.OK, Invoke: 0, Complete: 1}}},
		{"completes_before_it_is_invoked", history.History{{Process: 0, Key: "x", Kind: history.Read, Outcome: history.OK, Invoke: 10, Complete: 5}}},
		{"indeterminate_with_a_completion_time", history.History{{Process: 0, Key: "x", Kind: history.Read, Outcome: history.Info, Invoke: 0, Complete: 5}}},
		{"one_process_with_two_requests_in_flight", history.History{wr(0, 1, 0, 10), rd(0, 1, 5, 15)}},
		{"cas_to_the_value_it_already_expects", history.History{cas(0, 1, 1, false, 0, 10)}},
		{"operation_after_an_indeterminate_one", history.History{infoWr(0, 1, 0), rd(0, 1, 10, 20)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Check(c.ops, casReg, Options{})
			if err == nil {
				t.Fatalf("Check accepted a malformed history, verdict %v", got.Verdict)
			}
			if got.Verdict == Linearizable {
				t.Errorf("the Result returned alongside the error says %v; a caller that "+
					"ignores the error must not read a pass", got.Verdict)
			}
		})
	}
}

func TestCheckRejectsANilModel(t *testing.T) {
	if _, err := Check(history.History{rd(0, 0, 0, 1)}, nil, Options{}); err == nil {
		t.Fatal("Check accepted a nil model")
	}
	if _, err := CheckKey(history.History{rd(0, 0, 0, 1)}, nil, Options{}); err == nil {
		t.Fatal("CheckKey accepted a nil model")
	}
}

func TestCheckKeyRefusesMoreThanOneKey(t *testing.T) {
	h := history.History{rd(0, 0, 0, 10)}
	h = append(h, onKey("y", 5, rd(0, 0, 0, 10))...)
	if _, err := CheckKey(h, casReg, Options{}); err == nil {
		t.Fatal("CheckKey accepted operations on two different keys")
	}
}

func TestCheckKeyAgreesWithCheckOnASingleKey(t *testing.T) {
	for _, c := range corpus {
		if len(c.ops) == 0 {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			got, err := CheckKey(c.ops, c.model, Options{})
			if err != nil {
				t.Fatalf("CheckKey: %v", err)
			}
			if got.Verdict != c.want {
				t.Fatalf("CheckKey verdict = %v, want %v (%s)", got.Verdict, c.want, c.why)
			}
		})
	}
}

// violationTail is the classic violation, offset so it can be appended to or
// prefixed by other traffic.
func violationTail(at int64) history.History {
	return history.History{
		wr(90, 1, at, at+100),
		rd(91, 1, at+10, at+20),
		rd(92, 0, at+30, at+40),
	}
}

// cleanTraffic is a long, obviously linearizable run of one process writing a
// value and reading it back.
func cleanTraffic(proc int, from int64, pairs int) history.History {
	var h history.History
	t := from
	for i := 0; i < pairs; i++ {
		v := i%3 + 1
		h = append(h, wr(proc, v, t, t+5))
		h = append(h, rd(proc, v, t+5, t+10))
		t += 20
	}
	// End on a known value so the caller can reason about what follows.
	h = append(h, wr(proc, 0, t, t+5))
	return h
}

// TestCulpritWithoutMinimisation pins the other source of Result.Culprit. When
// no truncation was asked for, it is the operation the search got deepest with
// and still could not place - here the read of 0, which fails only after the
// write and the read of 1 have both been placed.
func TestCulpritWithoutMinimisation(t *testing.T) {
	h := history.History{wr(0, 1, 0, 10), rd(1, 1, 2, 4), rd(2, 0, 5, 8)}

	got, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != NotLinearizable {
		t.Fatalf("verdict = %v, want not linearizable", got.Verdict)
	}
	if got.Ops != nil {
		t.Errorf("Ops has %d entries, want none when Minimize is off", len(got.Ops))
	}
	if got.Culprit == nil {
		t.Fatal("no culprit reported; a violation with no operation to look at is not much of a report")
	}
	if got.Culprit.Kind != history.Read || got.Culprit.Observed != 0 || got.Culprit.Process != 2 {
		t.Errorf("culprit = %+v, want the read of 0 by process 2", *got.Culprit)
	}
	if !strings.Contains(got.Reason, "read returned 0") {
		t.Errorf("Reason = %q, want the model's explanation of that read", got.Reason)
	}
}

// TestReasonComesFromTheDeepestPointReached pins which of several failures
// gets reported.
//
// All three operations are mutually concurrent. The read of 5 can never be
// placed, and the search tries it three times: once from the initial state,
// once after the write, and once after the write and the read of 1. Reporting
// the first attempt would say the register held 0, which is true but useless -
// it describes the state the search started in rather than the state it got
// furthest with. The deepest attempt is the one worth printing.
func TestReasonComesFromTheDeepestPointReached(t *testing.T) {
	h := history.History{
		rd(0, 5, 0, 100), // impossible: nothing ever writes 5
		wr(1, 1, 0, 100),
		rd(2, 1, 0, 100),
	}

	got, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != NotLinearizable {
		t.Fatalf("verdict = %v, want not linearizable", got.Verdict)
	}
	if !strings.Contains(got.Reason, "read returned 5, but the register held 1") {
		t.Errorf("Reason = %q, want the explanation from the deepest state reached (1), not the initial one (0)", got.Reason)
	}
}

// TestMinimiseShrinksToTheViolation puts the violation first and a long clean
// run after it, which is the case where a time truncation genuinely shrinks.
func TestMinimiseShrinksToTheViolation(t *testing.T) {
	h := append(history.History{}, violationTail(0)...)
	h = append(h, cleanTraffic(1, 1000, 20)...)

	got, err := Check(h, casReg, Options{Minimize: true})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != NotLinearizable {
		t.Fatalf("verdict = %v, want not linearizable", got.Verdict)
	}
	if len(got.Ops) == 0 {
		t.Fatal("Minimize was set but no truncation was returned")
	}
	if len(got.Ops) > 10 {
		t.Errorf("the truncation kept %d of %d operations, want at most 10", len(got.Ops), len(h))
	}
	if got.Culprit == nil {
		t.Fatal("no culprit reported")
	}
	if got.Culprit.Kind != history.Read || got.Culprit.Observed != 0 {
		t.Errorf("culprit = %+v, want the read that observed 0", *got.Culprit)
	}
	// The truncation must itself still be a violation, or it is not a
	// counterexample.
	again, err := CheckKey(got.Ops, casReg, Options{})
	if err != nil {
		t.Fatalf("re-checking the truncation: %v", err)
	}
	if again.Verdict != NotLinearizable {
		t.Errorf("the returned truncation checks as %v; a counterexample must reproduce", again.Verdict)
	}
}

// TestMinimiseReturnsAPrefixWhenTheViolationIsLast records a real limit of the
// time-truncation approach: it can only ever return a prefix of the history, so
// a long clean run BEFORE the violation is kept in full. Deleting those early
// operations would not be sound - removing a write can turn a linearizable
// history into an unlinearizable one - so the checker does not try.
func TestMinimiseReturnsAPrefixWhenTheViolationIsLast(t *testing.T) {
	clean := cleanTraffic(1, 0, 20)
	h := append(history.History{}, clean...)
	h = append(h, violationTail(1000)...)

	got, err := Check(h, casReg, Options{Minimize: true})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != NotLinearizable {
		t.Fatalf("verdict = %v, want not linearizable", got.Verdict)
	}
	// A truncation shrinks in two ways: it drops operations invoked after the
	// cut, and it strips the results of operations still in flight at it. Here
	// only the second applies, because the violation is at the end.
	if sameOps(got.Ops, h.DropFailed()) {
		t.Errorf("the truncation is identical to the input; minimisation did nothing")
	}
	t.Logf("truncation kept %d of %d operations", len(got.Ops), len(h))
	if len(got.Ops) < len(clean) {
		t.Errorf("the truncation kept %d operations, fewer than the %d that precede the violation; "+
			"a time truncation cannot drop those", len(got.Ops), len(clean))
	}
	if got.Culprit == nil {
		t.Fatal("no culprit reported")
	}
	if got.Culprit.Kind != history.Read || got.Culprit.Observed != 0 {
		t.Errorf("culprit = %+v, want the read that observed 0", *got.Culprit)
	}
	// Ops must come back in invocation order, with the culprit last.
	for i := 1; i < len(got.Ops); i++ {
		if got.Ops[i-1].Invoke > got.Ops[i].Invoke {
			t.Fatalf("Ops is not sorted by invocation time at index %d", i)
		}
	}
	again, err := CheckKey(got.Ops, casReg, Options{})
	if err != nil {
		t.Fatalf("re-checking the truncation: %v", err)
	}
	if again.Verdict != NotLinearizable {
		t.Errorf("the returned truncation checks as %v; a counterexample must reproduce", again.Verdict)
	}
}

// TestMinimisationIsTight checks the other half of "minimal": one event time
// earlier, the truncation must linearize.
func TestMinimisationIsTight(t *testing.T) {
	h := append(history.History{}, violationTail(0)...)
	h = append(h, cleanTraffic(1, 1000, 5)...)

	got, err := Check(h, casReg, Options{Minimize: true})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != NotLinearizable || len(got.Ops) == 0 {
		t.Fatalf("verdict = %v with %d ops, want a not-linearizable truncation", got.Verdict, len(got.Ops))
	}

	// Rebuild the candidate event times exactly as the checker does, find the
	// truncation point it chose, and check the one below it passes.
	live := h.DropFailed()
	times := eventTimes(live)
	chosen := -1
	for i, tt := range times {
		if len(truncateAt(live, tt)) == len(got.Ops) && sameOps(truncateAt(live, tt), got.Ops) {
			chosen = i
			break
		}
	}
	if chosen <= 0 {
		t.Fatalf("could not locate the chosen truncation time (index %d of %d)", chosen, len(times))
	}
	prev := truncateAt(live, times[chosen-1])
	before, err := CheckKey(prev, casReg, Options{})
	if err != nil {
		t.Fatalf("checking the truncation one event earlier: %v", err)
	}
	if before.Verdict != Linearizable {
		t.Errorf("the truncation one event time earlier is %v, so the reported one is not minimal", before.Verdict)
	}
}

func sameOps(a, b history.History) bool {
	if len(a) != len(b) {
		return false
	}
	a = append(history.History{}, a...)
	b = append(history.History{}, b...)
	a.SortByInvoke()
	b.SortByInvoke()
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDeterminism(t *testing.T) {
	var h history.History
	h = append(h, onKey("alpha", 10, wr(0, 7, 0, 10), rd(1, 7, 20, 30))...)
	h = append(h, onKey("bravo", 20, violationTail(0)...)...)
	h = append(h, onKey("charlie", 30, wr(0, 3, 0, 10), rd(1, 0, 2, 4))...)

	first, err := Check(h, casReg, Options{Minimize: true})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	for i := 0; i < 5; i++ {
		again, err := Check(h, casReg, Options{Minimize: true})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if again.Verdict != first.Verdict || again.Key != first.Key ||
			len(again.Ops) != len(first.Ops) || again.Reason != first.Reason ||
			again.Visits != first.Visits {
			t.Fatalf("run %d differs:\nfirst = %v %q %d ops %q visits=%d\nagain = %v %q %d ops %q visits=%d",
				i, first.Verdict, first.Key, len(first.Ops), first.Reason, first.Visits,
				again.Verdict, again.Key, len(again.Ops), again.Reason, again.Visits)
		}
	}
}

func TestVerdictString(t *testing.T) {
	cases := map[Verdict]string{
		Linearizable:    "linearizable",
		NotLinearizable: "not linearizable",
		Unknown:         "unknown",
	}
	for v, want := range cases {
		if got := v.String(); got != want {
			t.Errorf("Verdict(%d).String() = %q, want %q", uint8(v), got, want)
		}
	}
	if Unknown == Linearizable {
		t.Fatal("Unknown and Linearizable must be distinct verdicts")
	}
}

func TestFailedOperationsAreDropped(t *testing.T) {
	// A Fail write definitely never happened, so the read of 0 stands. If the
	// checker kept it, the write would still be optional and the history would
	// pass anyway - so the assertion that bites is the second one, where the
	// failed write is the only thing that could have explained a read of 1.
	failed := history.Op{
		Process: 0, Key: "x", Kind: history.Write, Value: 1,
		Outcome: history.Fail, Invoke: 0, Complete: 10, Err: "connection refused",
	}
	ok, err := Check(history.History{failed, rd(1, 0, 20, 30)}, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if ok.Verdict != Linearizable {
		t.Errorf("verdict = %v, want linearizable", ok.Verdict)
	}

	bad, err := Check(history.History{failed, rd(1, 1, 20, 30)}, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if bad.Verdict != NotLinearizable {
		t.Errorf("verdict = %v, want not linearizable: a Fail write must not explain a read of 1", bad.Verdict)
	}
}

func TestPerKeyAccounting(t *testing.T) {
	var h history.History
	h = append(h, onKey("alpha", 10, wr(0, 7, 0, 10), rd(1, 7, 20, 30))...)
	h = append(h, onKey("bravo", 20, wr(0, 3, 0, 10))...)

	got, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict != Linearizable {
		t.Fatalf("verdict = %v, want linearizable", got.Verdict)
	}
	if len(got.PerKey) != 2 {
		t.Fatalf("PerKey has %d entries, want 2", len(got.PerKey))
	}
	if got.PerKey["alpha"].Ops != 2 || got.PerKey["bravo"].Ops != 1 {
		t.Errorf("PerKey op counts = %d and %d, want 2 and 1", got.PerKey["alpha"].Ops, got.PerKey["bravo"].Ops)
	}
	sum := got.PerKey["alpha"].Visits + got.PerKey["bravo"].Visits
	if got.Visits != sum {
		t.Errorf("Result.Visits = %d but the per-key visits sum to %d", got.Visits, sum)
	}
	if got.Key != "" {
		t.Errorf("Key = %q, want empty when nothing failed", got.Key)
	}
	// Elapsed is deliberately not asserted here. This history is checked in
	// microseconds and the monotonic clock on Windows is about a millisecond
	// coarse, so Elapsed legitimately reads zero. TestTimeoutIsUnknown asserts
	// it on a workload large enough for the clock to see.
}
