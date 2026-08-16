package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/checker"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

func TestParseInterleavedAcceptsFlagsOnEitherSide(t *testing.T) {
	// Go's flag package stops at the first non-flag argument, so
	// `check history.jsonl -expect linearizable` used to treat the expectation
	// as a second filename and refuse the command. It failed quietly enough to
	// be worth a test.
	cases := map[string][]string{
		"flags after the file":  {"history.jsonl", "-expect", "linearizable"},
		"flags before the file": {"-expect", "linearizable", "history.jsonl"},
		"flags on both sides":   {"-model", "register", "history.jsonl", "-expect", "linearizable"},
		"equals form":           {"history.jsonl", "-expect=linearizable"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			fs := flag.NewFlagSet("check", flag.ContinueOnError)
			fs.SetOutput(os.Stderr)
			expect := fs.String("expect", "any", "")
			modelName := fs.String("model", "cas-register", "")

			files, err := parseInterleaved(fs, args)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 || files[0] != "history.jsonl" {
				t.Fatalf("positional arguments = %v, want [history.jsonl]", files)
			}
			if *expect != "linearizable" {
				t.Fatalf("-expect = %q", *expect)
			}
			if name == "flags on both sides" && *modelName != "register" {
				t.Fatalf("-model = %q", *modelName)
			}
		})
	}
}

func TestParseInterleavedKeepsSeveralPositionals(t *testing.T) {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.String("expect", "any", "")

	files, err := parseInterleaved(fs, []string{"a.jsonl", "-expect", "any", "b.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	// The caller decides that two files is an error; this must not silently
	// drop one of them.
	if len(files) != 2 {
		t.Fatalf("positional arguments = %v, want two", files)
	}
}

func TestParseRange(t *testing.T) {
	for in, want := range map[string][2]int{
		"0-9":     {0, 9},
		"7":       {7, 7},
		" 3 - 5 ": {3, 5},
	} {
		lo, hi, err := parseRange(in)
		if err != nil {
			t.Errorf("parseRange(%q): %v", in, err)
			continue
		}
		if lo != want[0] || hi != want[1] {
			t.Errorf("parseRange(%q) = %d..%d, want %d..%d", in, lo, hi, want[0], want[1])
		}
	}
	for _, bad := range []string{"", "a-b", "9-0", "1-", "-"} {
		if _, _, err := parseRange(bad); err == nil {
			t.Errorf("parseRange(%q) was accepted", bad)
		}
	}
}

func TestCheckExpectation(t *testing.T) {
	// The exit status is the whole reason this tool can be a gate rather than
	// a report, so the mapping is pinned.
	cases := []struct {
		expect string
		got    checker.Verdict
		ok     bool
	}{
		{"linearizable", checker.Linearizable, true},
		{"linearizable", checker.NotLinearizable, false},
		{"linearizable", checker.Unknown, false},
		{"not-linearizable", checker.NotLinearizable, true},
		{"not-linearizable", checker.Linearizable, false},
		{"not-linearizable", checker.Unknown, false},
		{"any", checker.Linearizable, true},
		{"any", checker.NotLinearizable, true},
		{"any", checker.Unknown, false},
		{"", checker.NotLinearizable, true},
	}
	for _, c := range cases {
		err := checkExpectation(c.expect, c.got)
		if (err == nil) != c.ok {
			t.Errorf("expect=%q got=%s: err=%v, wanted ok=%v", c.expect, c.got, err, c.ok)
		}
	}
	if err := checkExpectation("nearly", checker.Linearizable); err == nil {
		t.Error("an unrecognised expectation was accepted; a typo would silently pass everything")
	}
}

func TestAnUndecidedSearchIsNeverASuccessfulExit(t *testing.T) {
	// The README says unknown is never collapsed into a pass "all the way out
	// to the exit code", and for a while that was not true: `check` defaults
	// to -expect any, and any absorbed unknown into a zero exit. A pipeline
	// reads the exit code, so that made the claim false where it mattered.
	//
	// "any" means either verdict. An exhausted search is the absence of one.
	for _, expect := range []string{"", "any", "linearizable", "not-linearizable"} {
		err := checkExpectation(expect, checker.Unknown)
		if err == nil {
			t.Fatalf("-expect %q accepted an undecided search", expect)
		}
		var undecided *noVerdict
		if !errors.As(err, &undecided) {
			t.Fatalf("-expect %q reported an undecided search as %T; it must exit 2, not 1", expect, err)
		}
		if !strings.Contains(err.Error(), "budget") {
			t.Errorf("the message does not say what to raise: %q", err)
		}
	}

	// And a real verdict must not be mistaken for one.
	for _, v := range []checker.Verdict{checker.Linearizable, checker.NotLinearizable} {
		var undecided *noVerdict
		if err := checkExpectation("any", v); errors.As(err, &undecided) {
			t.Fatalf("%s was reported as no verdict", v)
		}
	}
}

// TestASweepThatDecidedNothingIsNotAMismatch is the rule above, one level up,
// where it was not true.
//
// checkExpectation gets every seed right on its own. The sweep then counted the
// errors and returned one expectationFailed for the lot, so a sweep in which
// nothing was decided exited 1 - "the store contradicted what you asked" - and
// said so in words: `sweep -target kvsingle -seeds 1-2 -max-visits 1 -expect
// any` printed "unknown=2 mismatches=2", then "2 of 2 seeds did not match
// -expect any", and exited 1. Nothing about the store had been established.
//
// An undecided seed therefore wins over a decided mismatch, for the same reason
// checkExpectation tests for Unknown before it looks at -expect at all.
func TestASweepThatDecidedNothingIsNotAMismatch(t *testing.T) {
	var undecided *noVerdict
	var mismatch *expectationFailed

	// Two seeds, both undecided, under -expect any.
	if err := sweepOutcome("any", 2, 2, 2); !errors.As(err, &undecided) {
		t.Fatalf("a sweep that decided nothing reported %T (%v); an undecided search exits 2, not 1", err, err)
	}
	if err := sweepOutcome("any", 2, 2, 0); !errors.As(err, &undecided) {
		t.Fatalf("two undecided seeds reported %T (%v)", err, err)
	}
	// Mixed with real mismatches, the weaker claim is the only honest one: the
	// budget ran out, so the sweep does not know what the other seeds would
	// have said.
	if err := sweepOutcome("linearizable", 5, 1, 3); !errors.As(err, &undecided) {
		t.Fatalf("one undecided seed among the mismatches reported %T (%v)", err, err)
	}

	// A sweep that decided everything still reports a mismatch as one...
	if err := sweepOutcome("linearizable", 5, 0, 3); !errors.As(err, &mismatch) {
		t.Fatalf("three mismatched seeds reported %T (%v); that is exit 1", err, err)
	}
	// ...and a clean sweep is not an error at all.
	if err := sweepOutcome("linearizable", 5, 0, 0); err != nil {
		t.Fatalf("a sweep in which every seed matched returned %v", err)
	}
}

// TestCommittedFixtures is the repository's cheapest piece of evidence: two
// real histories, recorded from real processes over real sockets, whose
// verdicts anyone can reproduce without running anything. If a change to the
// checker or the history format ever alters what these mean, this fails.
func TestCommittedFixtures(t *testing.T) {
	cases := []struct {
		file string
		want checker.Verdict
		why  string
	}{
		{
			"split-brain.jsonl", checker.NotLinearizable,
			"recorded from kvsplit under a partition: a follower promoted itself and the two halves disagreed",
		},
		{
			"single-node-clean.jsonl", checker.Linearizable,
			"recorded from kvsingle under the same fault schedule; one mutex cannot be non-linearizable",
		},
	}
	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			path := filepath.Join("..", "..", "testdata", c.file)
			fh, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer fh.Close()

			h, err := history.Load(fh)
			if err != nil {
				t.Fatalf("%s: %v", c.file, err)
			}
			if len(h) == 0 {
				t.Fatalf("%s is empty", c.file)
			}
			if err := h.Validate(); err != nil {
				t.Fatalf("%s is not a well-formed history: %v", c.file, err)
			}

			res, err := checker.Check(h, model.CASRegister{}, checker.Options{
				MaxVisits: 5_000_000,
				Timeout:   60 * time.Second,
				Minimize:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if res.Verdict != c.want {
				t.Fatalf("%s: %s, want %s (%s)\n%s", c.file, res.Verdict, c.want, c.why, res.Reason)
			}
			t.Logf("%s: %d operations, %s, %d states in %s", c.file, len(h), res.Verdict, res.Visits, res.Elapsed)

			if c.want != checker.NotLinearizable {
				return
			}
			// A counterexample that cannot itself be re-checked is a
			// screenshot. The truncation keeps every operation invoked before
			// the cut and downgrades whatever was still in flight, so what
			// comes out is a valid history in its own right - and this is
			// where that claim gets tested rather than asserted.
			if len(res.Ops) == 0 {
				t.Fatal("no counterexample was produced")
			}
			if err := res.Ops.Validate(); err != nil {
				t.Fatalf("the counterexample is not a well-formed history: %v", err)
			}
			again, err := checker.Check(res.Ops, model.CASRegister{}, checker.Options{MaxVisits: 5_000_000})
			if err != nil {
				t.Fatal(err)
			}
			if again.Verdict != checker.NotLinearizable {
				t.Fatalf("the counterexample checks out as %s on its own", again.Verdict)
			}
		})
	}
}

func TestRenderOpSaysWhatHappened(t *testing.T) {
	cases := []struct {
		op   history.Op
		want []string
	}{
		{
			history.Op{Process: 3, Key: "k0", Kind: history.Read, Observed: 42, Outcome: history.OK, Invoke: 1_000_000, Complete: 2_000_000},
			[]string{"p3@c0", "read", "k0", "= 42"},
		},
		{
			history.Op{Process: 1, Key: "k1", Kind: history.Write, Value: 7, Outcome: history.OK, Invoke: 0, Complete: 1},
			[]string{"write k1 = 7", "ok"},
		},
		{
			history.Op{Process: 2, Key: "k2", Kind: history.CAS, From: 1, To: 2, Swapped: true, Outcome: history.OK, Invoke: 0, Complete: 1},
			[]string{"cas", "1 -> 2", "swapped"},
		},
		{
			history.Op{Process: 4, Key: "k3", Kind: history.Write, Value: 9, Outcome: history.Info, Invoke: 0, Complete: history.Pending, Err: "timeout"},
			// An unanswered operation must not render a completion time it
			// does not have, or a reader will believe it finished.
			[]string{"indeterminate", "timeout"},
		},
	}
	for _, c := range cases {
		got := renderOp(c.op, "@c0")
		for _, want := range c.want {
			if !strings.Contains(got, want) {
				t.Errorf("renderOp(%s) = %q, missing %q", c.op.Kind, got, want)
			}
		}
	}
	pending := renderOp(history.Op{Key: "k0", Kind: history.Read, Outcome: history.Info, Complete: history.Pending}, "")
	if strings.Contains(pending, "9223372036854775807") || strings.Contains(pending, "2562047h") {
		t.Errorf("an unanswered operation rendered its sentinel completion time: %q", pending)
	}
}

// TestImpossibleFlagsNeverStartACluster is the defect this fix was written for.
//
// Six flags used to be silently replaced by a default while the banner echoed
// what the user had typed: `-duration -5s` printed "duration=-5s" and then ran
// the eight-second configuration. A value the harness cannot honour has to be
// refused, by name, before anything is built, started or printed.
//
// The probe is -bin-dir pointing at an empty directory. A run that got as far
// as starting nodes fails there naming the binary it could not find, so the two
// outcomes cannot be confused - and the control at the end proves the probe
// really does fire when nothing refuses the flags first.
func TestImpossibleFlagsNeverStartACluster(t *testing.T) {
	empty := t.TempDir()

	// Zero is meaningless for a count, a run length or a timeout; for a pause
	// it means "none" and is tested as honoured elsewhere. Negatives are
	// meaningless everywhere.
	cases := []struct{ flag, value string }{
		{"-duration", "-5s"},
		{"-duration", "0"},
		{"-keys", "0"},
		{"-clients", "0"},
		{"-op-timeout", "0"},
		{"-quiesce", "-1s"},
		{"-think", "-3s"},
		{"-value-max", "1"},
		{"-max-ops", "-1"},
	}
	commands := map[string]func(context.Context, []string) error{
		"run":   cmdRun,
		"sweep": cmdSweep,
	}
	for name, cmd := range commands {
		for _, c := range cases {
			t.Run(name+" "+c.flag+" "+c.value, func(t *testing.T) {
				err := cmd(t.Context(), []string{"-target", "kvsingle", "-bin-dir", empty, c.flag, c.value})
				if err == nil {
					t.Fatalf("%s accepted %s %s", name, c.flag, c.value)
				}
				if !strings.Contains(err.Error(), c.flag) {
					t.Fatalf("%s failed with %q, which never names %s; the value was replaced rather than refused",
						name, err, c.flag)
				}
			})
		}
	}

	t.Run("control: nothing else stops the run reaching the cluster", func(t *testing.T) {
		err := cmdRun(t.Context(), []string{"-target", "kvsingle", "-bin-dir", empty, "-duration", "5s"})
		if err == nil || !strings.Contains(err.Error(), "cluster") {
			t.Fatalf("the probe does not detect a cluster start: %v", err)
		}
	})
}

// TestZeroPausesAreAcceptedAndHonoured is the exception that is not an
// exception: zero is meaningless for a count or a run length, but for a pause
// it means "none", which is a thing a person can genuinely want. Refusing it
// would be as wrong as replacing it, and replacing it is what used to happen -
// the harness reads a zero as a blank field unless it is told otherwise.
func TestZeroPausesAreAcceptedAndHonoured(t *testing.T) {
	f := parseRunFlags(t, "-think", "0", "-quiesce", "0")
	cfg := f.harnessConfig(1).WithDefaults()
	if cfg.ThinkMax != 0 {
		t.Errorf("-think 0 became %s", cfg.ThinkMax)
	}
	if cfg.Quiesce != 0 {
		t.Errorf("-quiesce 0 became %s", cfg.Quiesce)
	}

	// And the flags nobody typed still get the harness's own choice, or the
	// fix above would have turned every default run into a hot loop.
	untouched := parseRunFlags(t).harnessConfig(1).WithDefaults()
	if untouched.ThinkMax <= 0 || untouched.Quiesce <= 0 {
		t.Errorf("a run that asked for nothing got think=%s quiesce=%s", untouched.ThinkMax, untouched.Quiesce)
	}
}

// parseRunFlags parses args the way run and sweep do, and fails the test if
// they are refused.
func parseRunFlags(t *testing.T, args ...string) *runFlags {
	t.Helper()
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	f := &runFlags{}
	f.register(fs)
	if err := f.parse(fs, args); err != nil {
		t.Fatalf("%v was refused: %v", args, err)
	}
	return f
}

func TestPlural(t *testing.T) {
	if got := plural(1, "key"); got != "1 key" {
		t.Errorf("plural(1) = %q", got)
	}
	if got := plural(2, "key"); got != "2 keys" {
		t.Errorf("plural(2) = %q", got)
	}
}
