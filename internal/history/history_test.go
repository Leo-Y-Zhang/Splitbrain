package history

import (
	"bytes"
	"strings"
	"testing"
)

// op is a terse constructor so the tables below read as histories rather than
// as struct literals.
func op(proc int, key string, k Kind, oc Outcome, inv, comp int64) Op {
	o := Op{Process: proc, Key: key, Kind: k, Outcome: oc, Invoke: inv, Complete: comp}
	if oc == Info {
		o.Complete = Pending
	}
	return o
}

func TestKindRoundTrip(t *testing.T) {
	for _, k := range []Kind{Read, Write, CAS} {
		got, err := ParseKind(k.String())
		if err != nil {
			t.Fatalf("ParseKind(%q): %v", k.String(), err)
		}
		if got != k {
			t.Fatalf("ParseKind(%q) = %v, want %v", k.String(), got, k)
		}
	}
	if _, err := ParseKind("increment"); err == nil {
		t.Fatal("ParseKind accepted an unknown kind; an unrecognised operation must not become a silent read")
	}
}

func TestOutcomeRoundTrip(t *testing.T) {
	for _, oc := range []Outcome{OK, Fail, Info} {
		got, err := ParseOutcome(oc.String())
		if err != nil {
			t.Fatalf("ParseOutcome(%q): %v", oc.String(), err)
		}
		if got != oc {
			t.Fatalf("ParseOutcome(%q) = %v, want %v", oc.String(), got, oc)
		}
	}
	if _, err := ParseOutcome("maybe"); err == nil {
		t.Fatal("ParseOutcome accepted an unknown outcome")
	}
}

func TestConcurrentUsesStrictInequality(t *testing.T) {
	// Two operations that merely touch at an instant are NOT concurrent. The
	// checker's entry ordering depends on this, so it is pinned here.
	a := op(0, "x", Write, OK, 0, 10)
	b := op(1, "x", Read, OK, 10, 20)
	if a.Concurrent(b) || b.Concurrent(a) {
		t.Fatal("operations that abut at a single instant must not count as concurrent")
	}

	c := op(1, "x", Read, OK, 9, 20)
	if !a.Concurrent(c) || !c.Concurrent(a) {
		t.Fatal("overlapping operations must count as concurrent, in both directions")
	}
}

func TestConcurrentWithPending(t *testing.T) {
	// A pending operation overlaps everything invoked after it, which is why it
	// can be linearized arbitrarily late.
	pending := op(0, "x", Write, Info, 0, 0)
	later := op(1, "x", Read, OK, 1_000_000, 2_000_000)
	if !pending.Concurrent(later) {
		t.Fatal("a pending operation must be concurrent with everything invoked after it")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	in := History{
		{Process: 0, Key: "x", Kind: Write, Value: 7, Outcome: OK, Invoke: 100, Complete: 200},
		{Process: 1, Key: "x", Kind: Read, Observed: 7, Outcome: OK, Invoke: 300, Complete: 400},
		{Process: 2, Key: "y", Kind: CAS, From: 0, To: 3, Swapped: true, Outcome: OK, Invoke: 500, Complete: 600},
		{Process: 3, Key: "y", Kind: Write, Value: 9, Outcome: Info, Invoke: 700, Complete: Pending, Err: "timeout"},
		{Process: 4, Key: "y", Kind: Read, Outcome: Fail, Invoke: 800, Complete: 810, Err: "connection refused"},
	}

	var buf bytes.Buffer
	if err := in.Save(&buf); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("round trip changed the length: %d -> %d", len(in), len(out))
	}
	for i := range in {
		if in[i] != out[i] {
			t.Errorf("op %d changed:\n in  %+v\n out %+v", i, in[i], out[i])
		}
	}
}

func TestSaveOmitsIrrelevantFields(t *testing.T) {
	// A history file is meant to be read by a person. A read should not carry a
	// "from"/"to", and an indeterminate operation must not carry a completion
	// time it does not have.
	h := History{
		{Process: 0, Key: "x", Kind: Read, Observed: 4, Outcome: OK, Invoke: 1, Complete: 2},
		{Process: 1, Key: "x", Kind: Write, Value: 5, Outcome: Info, Invoke: 3, Complete: Pending},
	}
	var buf bytes.Buffer
	if err := h.Save(&buf); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d: %q", len(lines), buf.String())
	}
	for _, banned := range []string{`"from"`, `"to"`, `"swapped"`} {
		if strings.Contains(lines[0], banned) {
			t.Errorf("read line carries %s: %s", banned, lines[0])
		}
	}
	if strings.Contains(lines[1], "complete_ns") {
		t.Errorf("indeterminate line carries a completion time: %s", lines[1])
	}
	if !strings.Contains(lines[1], `"outcome":"info"`) {
		t.Errorf("indeterminate line lost its outcome: %s", lines[1])
	}
}

func TestLoadRejectsMalformedLines(t *testing.T) {
	cases := map[string]string{
		"broken json":                `{"process":0,`,
		"unknown kind":               `{"process":0,"key":"x","kind":"append","outcome":"ok","invoke_ns":1,"complete_ns":2}`,
		"unknown outcome":            `{"process":0,"key":"x","kind":"read","outcome":"probably","invoke_ns":1,"complete_ns":2}`,
		"ok with no completion time": `{"process":0,"key":"x","kind":"read","outcome":"ok","invoke_ns":1}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			// Silently skipping an unparseable line would produce a verdict
			// about a history nobody wrote.
			if _, err := Load(strings.NewReader(line + "\n")); err == nil {
				t.Fatal("Load accepted a malformed line")
			}
		})
	}
}

func TestLoadSkipsBlankLines(t *testing.T) {
	in := "\n" + `{"process":0,"key":"x","kind":"read","outcome":"ok","invoke_ns":1,"complete_ns":2,"observed":0}` + "\n\n"
	h, err := Load(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 1 {
		t.Fatalf("want 1 op, got %d", len(h))
	}
}

func TestSaveSortsByInvoke(t *testing.T) {
	h := History{
		{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 300, Complete: 400},
		{Process: 1, Key: "x", Kind: Read, Outcome: OK, Invoke: 100, Complete: 200},
	}
	var buf bytes.Buffer
	if err := h.Save(&buf); err != nil {
		t.Fatal(err)
	}
	out, err := Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Invoke != 100 {
		t.Fatalf("Save did not sort by invocation time: first op invoked at %d", out[0].Invoke)
	}
	// The caller's slice must not be reordered underneath them.
	if h[0].Invoke != 300 {
		t.Fatal("Save mutated the caller's slice order")
	}
}

func TestByKeySplitsCleanly(t *testing.T) {
	h := History{
		op(0, "x", Read, OK, 1, 2),
		op(1, "y", Read, OK, 3, 4),
		op(2, "x", Write, OK, 5, 6),
	}
	byKey := h.ByKey()
	if len(byKey) != 2 {
		t.Fatalf("want 2 keys, got %d", len(byKey))
	}
	if len(byKey["x"]) != 2 || len(byKey["y"]) != 1 {
		t.Fatalf("wrong split: x=%d y=%d", len(byKey["x"]), len(byKey["y"]))
	}
	keys := h.Keys()
	if len(keys) != 2 || keys[0] != "x" || keys[1] != "y" {
		t.Fatalf("Keys() = %v, want sorted [x y]", keys)
	}
}

func TestDropFailedKeepsInfo(t *testing.T) {
	// The single most important rule in the package: Fail goes, Info stays.
	h := History{
		op(0, "x", Read, OK, 1, 2),
		op(1, "x", Read, Fail, 3, 4),
		op(2, "x", Write, Info, 5, 0),
	}
	got := h.DropFailed()
	if len(got) != 2 {
		t.Fatalf("want 2 ops after dropping failures, got %d", len(got))
	}
	var sawInfo bool
	for _, o := range got {
		if o.Outcome == Fail {
			t.Error("a definitely-failed operation survived")
		}
		if o.Outcome == Info {
			sawInfo = true
		}
	}
	if !sawInfo {
		t.Fatal("an indeterminate operation was dropped; it may have taken effect, so removing it makes the checker unsound")
	}
}

func TestCounts(t *testing.T) {
	h := History{
		op(0, "x", Read, OK, 1, 2),
		op(1, "x", Read, OK, 3, 4),
		op(2, "x", Read, Fail, 5, 6),
		op(3, "x", Write, Info, 7, 0),
	}
	ok, fail, info := h.Counts()
	if ok != 2 || fail != 1 || info != 1 {
		t.Fatalf("Counts() = %d/%d/%d, want 2/1/1", ok, fail, info)
	}
}

func TestValidateAcceptsAWellFormedHistory(t *testing.T) {
	h := History{
		op(0, "x", Read, OK, 0, 10),
		op(0, "x", Write, OK, 10, 20), // abutting is fine: not overlapping
		op(1, "x", Read, OK, 5, 15),   // a different process may overlap
		op(2, "y", Write, Info, 30, 0),
	}
	if err := h.Validate(); err != nil {
		t.Fatalf("Validate rejected a well-formed history: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]History{
		"negative invoke":         {{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: -1, Complete: 1}},
		"empty key":               {{Process: 0, Key: "", Kind: Read, Outcome: OK, Invoke: 0, Complete: 1}},
		"completes before invoke": {{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 10, Complete: 5}},
		"ok without completion":   {{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 0, Complete: Pending}},
		"info with completion":    {{Process: 0, Key: "x", Kind: Read, Outcome: Info, Invoke: 0, Complete: 5}},
		"no-op cas":               {{Process: 0, Key: "x", Kind: CAS, From: 3, To: 3, Outcome: OK, Invoke: 0, Complete: 1}},
		"one process, two in flight": {
			{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 0, Complete: 100},
			{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 50, Complete: 150},
		},
		"a process continues after an indeterminate operation": {
			{Process: 0, Key: "x", Kind: Write, Outcome: Info, Invoke: 0, Complete: Pending},
			{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 10, Complete: 20},
		},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if err := h.Validate(); err == nil {
				t.Fatal("Validate accepted a history it must reject")
			}
		})
	}
}

func TestValidateExplainsWhyAProcessMustRetire(t *testing.T) {
	// An operation after an indeterminate one always overlaps it too, because
	// a pending operation never completes, so the generic overlap check would
	// reject this history anyway. The dedicated branch exists only to say why,
	// and "one client cannot have two requests in flight" is the wrong reason:
	// the client is not being greedy, it simply cannot know its last request
	// is finished. A message that sends the reader after the wrong bug is
	// worth a test of its own.
	h := History{
		{Process: 0, Key: "x", Kind: Write, Outcome: Info, Invoke: 0, Complete: Pending},
		{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 10, Complete: 20},
	}
	err := h.Validate()
	if err == nil {
		t.Fatal("Validate accepted an operation issued after an indeterminate one")
	}
	if !strings.Contains(err.Error(), "indeterminate") {
		t.Fatalf("Validate rejected it as %q, which does not name the real reason", err)
	}
}

func TestValidateCatchesOverlapRegardlessOfSliceOrder(t *testing.T) {
	// The overlap check must not depend on the caller having sorted the slice.
	h := History{
		{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 50, Complete: 150},
		{Process: 0, Key: "x", Kind: Read, Outcome: OK, Invoke: 0, Complete: 100},
	}
	if err := h.Validate(); err == nil {
		t.Fatal("Validate missed overlapping operations because they were out of order in the slice")
	}
}

func TestPendingIsLargerThanAnyRealTimestamp(t *testing.T) {
	// Sorting by completion time relies on this, and a smaller sentinel would
	// silently reorder pending operations into the middle of a history.
	if Pending <= 1<<62 {
		t.Fatalf("Pending = %d is not comfortably larger than any run duration", Pending)
	}
}
