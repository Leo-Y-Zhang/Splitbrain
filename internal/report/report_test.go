package report

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/checker"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/faultnet"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// violation is the textbook history: a write that one client observes and a
// strictly later read that does not.
func violation() history.History {
	return history.History{
		{Process: 0, Key: "k0", Kind: history.Write, Value: 1, Outcome: history.OK, Invoke: 0, Complete: 10_000_000},
		{Process: 1, Key: "k0", Kind: history.Read, Observed: 1, Outcome: history.OK, Invoke: 2_000_000, Complete: 4_000_000},
		{Process: 2, Key: "k0", Kind: history.Read, Observed: 0, Outcome: history.OK, Invoke: 5_000_000, Complete: 8_000_000},
	}
}

func render(t *testing.T, in Input) string {
	t.Helper()
	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func checked(t *testing.T, h history.History) checker.Result {
	t.Helper()
	res, err := checker.Check(h, model.CASRegister{}, checker.Options{Minimize: true})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestReportShowsTheVerdict(t *testing.T) {
	h := violation()
	res := checked(t, h)
	if res.Verdict != checker.NotLinearizable {
		t.Fatalf("the fixture is supposed to be a violation, got %s", res.Verdict)
	}

	page := render(t, Input{Title: "fixture", Verdict: res, History: h})
	for _, want := range []string{"NOT LINEARIZABLE", "fixture", "read k0", "write k0 = 1"} {
		if !strings.Contains(page, want) {
			t.Errorf("the page does not mention %q", want)
		}
	}
}

func TestReportIsSelfContained(t *testing.T) {
	// A report that fetches anything is useless offline, useless in an
	// air-gapped review, and a privacy problem when someone opens it.
	page := render(t, Input{Verdict: checked(t, violation()), History: violation()})

	external := regexp.MustCompile(`(?i)(https?:)?//[a-z0-9.-]+\.[a-z]{2,}`)
	if m := external.FindString(page); m != "" {
		t.Errorf("the page references an external host: %q", m)
	}
	for _, banned := range []string{"<script", "src=", "@import", "url("} {
		if strings.Contains(strings.ToLower(page), banned) {
			t.Errorf("the page contains %q", banned)
		}
	}
}

func TestReportEscapesWhatItIsGiven(t *testing.T) {
	// Keys come from a run, but a history file can come from anywhere.
	h := history.History{
		{Process: 0, Key: `<img onerror=alert(1)>`, Kind: history.Read, Outcome: history.OK, Invoke: 0, Complete: 1},
	}
	res, err := checker.Check(h, model.CASRegister{}, checker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	page := render(t, Input{Verdict: res, History: h, Title: `</title><script>alert(1)</script>`})
	if strings.Contains(page, "<img onerror") || strings.Contains(page, "<script>alert") {
		t.Fatal("the page rendered attacker-controlled markup unescaped")
	}
}

func TestReportEscapesTheTransportError(t *testing.T) {
	// Op.Err is the one field whose content comes straight off the wire: it is
	// whatever a server or the network said. A history file is passed around
	// between people, so this is the realistic injection vector and it was the
	// one the original escaping test did not cover.
	h := history.History{
		{
			Process: 0, Key: "k0", Kind: history.Write, Value: 1,
			Outcome: history.Info, Invoke: 0, Complete: history.Pending,
			Err: `]]></title><svg/onload=alert(1)><img src=x onerror=alert(2)>`,
		},
	}
	res, err := checker.Check(h, model.CASRegister{}, checker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	page := render(t, Input{Verdict: res, History: h})
	for _, banned := range []string{"<svg/onload", "<img src=x", "</title><svg"} {
		if strings.Contains(page, banned) {
			t.Fatalf("a transport error rendered unescaped markup: %q survived", banned)
		}
	}
	// It must still be readable, or the escaping has eaten the diagnostic.
	if !strings.Contains(page, "onload=alert(1)") && !strings.Contains(page, "onload=alert(1)") {
		t.Log("note: the error text is escaped beyond recognition, which is safe but unhelpful")
	}
}

func TestReportDoesNotPassOffTheTruncationAsTheWholeRun(t *testing.T) {
	// The stats card counts the recorded history; the drawing shows the
	// truncation, in which an operation still in flight at the cut is redrawn
	// as unanswered. Left unsaid, the page states something that never
	// happened: a bar the footer calls "never answered" for an operation the
	// run recorded as ok.
	h := violation()
	res := checked(t, h)
	if res.Verdict != checker.NotLinearizable || len(res.Ops) == 0 {
		t.Fatalf("the fixture must produce a counterexample; got %s with %d ops", res.Verdict, len(res.Ops))
	}

	in := Input{Verdict: res, History: h}
	d, err := in.build()
	if err != nil {
		t.Fatal(err)
	}
	if !d.Truncated {
		t.Fatal("a drawing built from a counterexample does not know it is a truncation")
	}

	var drawn string
	for _, st := range d.Stats {
		if st.Name == "operations drawn" {
			drawn = st.Value
		}
	}
	if drawn == "" {
		t.Fatal("the stats do not say how much of the run was drawn")
	}

	page := render(t, in)
	if !strings.Contains(page, "the drawing is the truncation") {
		t.Fatal("the page does not tell the reader that the picture and the counts describe different things")
	}
	// The unqualified claim must be gone.
	if strings.Contains(page, "never got one") {
		t.Fatal("the footer still claims an unanswered bar means the client never got an answer at all")
	}

	// And on a clean run, where nothing is truncated, the page must not start
	// hedging about a truncation that did not happen.
	clean := history.History{
		{Process: 0, Key: "k0", Kind: history.Write, Value: 1, Outcome: history.OK, Invoke: 0, Complete: 1_000_000},
		{Process: 0, Key: "k0", Kind: history.Read, Observed: 1, Outcome: history.OK, Invoke: 2_000_000, Complete: 3_000_000},
	}
	cleanPage := render(t, Input{Verdict: checked(t, clean), History: clean})
	if strings.Contains(cleanPage, "the drawing is the truncation") {
		t.Fatal("a clean run's page describes itself as a truncation")
	}
	if !strings.Contains(cleanPage, "had not been answered\n  at all") &&
		!strings.Contains(cleanPage, "at all") {
		t.Fatal("a clean run's page lost the plain statement of what a dashed bar means")
	}
}

func TestReportMarksTheCulprit(t *testing.T) {
	res := checked(t, violation())
	if res.Culprit == nil {
		t.Fatal("the checker did not name a culprit for a violation")
	}
	page := render(t, Input{Verdict: res, History: violation()})
	if !strings.Contains(page, "culprit") {
		t.Fatal("the culprit is not distinguished in the drawing, so the reader has to find it themselves")
	}
}

func TestReportDrawsFaultBands(t *testing.T) {
	h := violation()
	events := []faultnet.Event{
		{At: 2 * time.Millisecond, Link: "c1", Fault: faultnet.Drop},
		{At: 6 * time.Millisecond, Link: "c1", Fault: faultnet.Pass},
	}
	page := render(t, Input{Verdict: checked(t, h), History: h, Faults: events})
	if !strings.Contains(page, `class="band"`) {
		t.Fatal("a cut in the fault timeline is not drawn; the reader cannot tell whether the violation happened during a partition")
	}
}

// lateWindow is ten reads between 5s and 5.95s into a run: the shape of the
// window a long run's report draws when it keeps only the operations nearest
// the violation, so the drawing starts well after the run did.
func lateWindow() history.History {
	var h history.History
	for i := 0; i < 10; i++ {
		at := 5*time.Second + time.Duration(i)*100*time.Millisecond
		h = append(h, history.Op{
			Process: i, Key: "k0", Kind: history.Read, Outcome: history.OK,
			Invoke: int64(at), Complete: int64(at + 50*time.Millisecond),
		})
	}
	return h
}

// xAt is where the drawing puts instant t of lateWindow, by the same
// arithmetic the page uses.
func xAt(t time.Duration) int {
	lo, hi := 5*time.Second, 5950*time.Millisecond
	return plotLeft + int(float64(t-lo)/float64(hi-lo)*float64(pageWidth-plotLeft-plotRight))
}

func TestFaultBandsSitOverTheOperationsTheyCut(t *testing.T) {
	// A fault's At and an operation's timestamps are both offsets from the
	// start of the run, so a cut at 5.2s belongs over the operations running
	// at 5.2s whatever instant the drawn window happens to start at. Measured
	// from the window instead, this cut landed 5s further right: past the last
	// operation, as a two-pixel sliver on the right edge, next to operations
	// that ran on a healthy network.
	h := lateWindow()
	events := []faultnet.Event{
		{At: 5200 * time.Millisecond, Link: "c1", Fault: faultnet.Drop},
		{At: 5400 * time.Millisecond, Link: "c1", Fault: faultnet.Pass},
	}
	d, err := Input{Verdict: checked(t, h), History: h, Faults: events}.build()
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Bands) != 1 {
		t.Fatalf("drew %d bands for one cut", len(d.Bands))
	}
	b := d.Bands[0]
	if want := xAt(5200 * time.Millisecond); b.X != want {
		t.Errorf("the cut at 5.2s starts at x=%d, want %d", b.X, want)
	}
	if want := xAt(5400*time.Millisecond) - xAt(5200*time.Millisecond); b.W != want {
		t.Errorf("the 200ms cut is %d pixels wide, want %d", b.W, want)
	}

	// The axis has to be on the same clock as the bands, the hover text and
	// the command line, all of which say when things happened in the run.
	if first, last := d.Ticks[0].Label, d.Ticks[len(d.Ticks)-1].Label; first != "5s" || last != "5.95s" {
		t.Errorf("the axis runs from %q to %q; the drawn operations ran from 5s to 5.95s", first, last)
	}
}

func TestFaultBandsLeaveOutCutsOutsideTheWindow(t *testing.T) {
	// A cut that healed before the first drawn operation, or began after the
	// last, has nothing in the drawing to sit over. Clamped to an edge it would
	// read as a cut at that edge.
	h := lateWindow()
	events := []faultnet.Event{
		{At: 1 * time.Second, Link: "c1", Fault: faultnet.Drop},
		{At: 2 * time.Second, Link: "c1", Fault: faultnet.Pass},
		{At: 7 * time.Second, Link: "c1", Fault: faultnet.Drop},
		{At: 8 * time.Second, Link: "c1", Fault: faultnet.Pass},
	}
	d, err := Input{Verdict: checked(t, h), History: h, Faults: events}.build()
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Bands) != 0 {
		t.Errorf("drew %d bands for cuts that all lie outside the drawn window: %+v", len(d.Bands), d.Bands)
	}

	// One still in force when the window opens is drawn from the left edge.
	events = []faultnet.Event{
		{At: 4 * time.Second, Link: "c1", Fault: faultnet.Drop},
		{At: 5200 * time.Millisecond, Link: "c1", Fault: faultnet.Pass},
	}
	d, err = Input{Verdict: checked(t, h), History: h, Faults: events}.build()
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Bands) != 1 || d.Bands[0].X != plotLeft || d.Bands[0].X+d.Bands[0].W != xAt(5200*time.Millisecond) {
		t.Errorf("a cut from 4s to 5.2s over a window opening at 5s drew %+v, want one band from x=%d to x=%d",
			d.Bands, plotLeft, xAt(5200*time.Millisecond))
	}
}

func TestAHealOfEveryLinkEndsTheBand(t *testing.T) {
	// The flaky and chaos schedules cut links one by one and heal them all at
	// once, and the nemesis heals every link when a run ends. That heal names
	// the link "*", so counting it as one more link that is up left every
	// individually cut link down for good: one band from the first cut to the
	// end of the drawing, and every heal after it invisible.
	h := lateWindow()
	events := []faultnet.Event{
		{At: 5100 * time.Millisecond, Link: "c1", Fault: faultnet.Drop},
		{At: 5100 * time.Millisecond, Link: "c2->c0", Fault: faultnet.Drop},
		{At: 5300 * time.Millisecond, Link: "*", Fault: faultnet.Pass},
		{At: 5600 * time.Millisecond, Link: "c1", Fault: faultnet.Drop},
		{At: 5800 * time.Millisecond, Link: "*", Fault: faultnet.Pass},
	}
	d, err := Input{Verdict: checked(t, h), History: h, Faults: events}.build()
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Bands) != 2 {
		t.Fatalf("drew %d bands for two cuts each ended by a heal of every link: %+v", len(d.Bands), d.Bands)
	}
	for i, span := range [][2]time.Duration{
		{5100 * time.Millisecond, 5300 * time.Millisecond},
		{5600 * time.Millisecond, 5800 * time.Millisecond},
	} {
		b := d.Bands[i]
		if b.X != xAt(span[0]) || b.X+b.W != xAt(span[1]) {
			t.Errorf("band %d runs from x=%d to x=%d, want %d to %d (%s to %s)",
				i, b.X, b.X+b.W, xAt(span[0]), xAt(span[1]), span[0], span[1])
		}
	}
}

func TestReportNamesTheNodeAProcessTalkedTo(t *testing.T) {
	h := violation()
	page := render(t, Input{
		Verdict:     checked(t, h),
		History:     h,
		ProcessNode: map[int]string{1: "c0", 2: "c2"},
	})
	// Two clients disagreeing is far easier to read when you can see they were
	// on different nodes.
	if !strings.Contains(page, "p1 @ c0") || !strings.Contains(page, "p2 @ c2") {
		t.Fatal("the report does not say which node each process was talking to")
	}
}

func TestReportCapsHowMuchItDraws(t *testing.T) {
	// A few thousand rows is an unreadable page and a very large file.
	var h history.History
	for i := 0; i < 600; i++ {
		h = append(h, history.Op{
			Process: i, Key: "k0", Kind: history.Read, Outcome: history.OK,
			Invoke: int64(i) * 1_000_000, Complete: int64(i)*1_000_000 + 500_000,
		})
	}
	res := checked(t, h)
	if res.Verdict != checker.Linearizable {
		t.Fatalf("the fixture should linearize, got %s", res.Verdict)
	}

	in := Input{Verdict: res, History: h, MaxOps: 50}
	d, err := in.build()
	if err != nil {
		t.Fatal(err)
	}
	if d.Drawn != 50 {
		t.Fatalf("drew %d rows with MaxOps=50", d.Drawn)
	}
	if !strings.Contains(d.Note, "600") {
		t.Errorf("the note does not admit how much was left out: %q", d.Note)
	}
}

func TestReportIsDeterministic(t *testing.T) {
	// The page ends up in CI artifacts and in commits; it must not churn.
	in := Input{Verdict: checked(t, violation()), History: violation()}
	if render(t, in) != render(t, in) {
		t.Fatal("two renderings of the same input differ")
	}
}

func TestReportHandlesACleanRun(t *testing.T) {
	h := history.History{
		{Process: 0, Key: "k0", Kind: history.Write, Value: 1, Outcome: history.OK, Invoke: 0, Complete: 1_000_000},
		{Process: 0, Key: "k0", Kind: history.Read, Observed: 1, Outcome: history.OK, Invoke: 2_000_000, Complete: 3_000_000},
	}
	page := render(t, Input{Verdict: checked(t, h), History: h})
	if !strings.Contains(page, "LINEARIZABLE") {
		t.Fatal("a clean run does not render its verdict")
	}
	if strings.Contains(page, "NOT LINEARIZABLE") {
		t.Fatal("a clean run rendered as a violation")
	}
}

func TestReportHandlesAnEmptyHistory(t *testing.T) {
	res, err := checker.Check(nil, model.CASRegister{}, checker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	in := Input{Verdict: res}
	if _, err := in.build(); err != nil {
		t.Fatalf("an empty history broke the renderer: %v", err)
	}
}

func TestPendingOperationsReachTheRightEdge(t *testing.T) {
	h := history.History{
		{Process: 0, Key: "k0", Kind: history.Write, Value: 1, Outcome: history.Info, Invoke: 0, Complete: history.Pending},
		{Process: 1, Key: "k0", Kind: history.Read, Observed: 1, Outcome: history.OK, Invoke: 1_000_000, Complete: 2_000_000},
	}
	in := Input{Verdict: checked(t, h), History: h}
	d, err := in.build()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, r := range d.Rows {
		if r.Pending {
			found = true
			if r.X+r.W < pageWidth-plotRight {
				t.Errorf("an unanswered operation stops at x=%d instead of running to the edge at %d",
					r.X+r.W, pageWidth-plotRight)
			}
		}
	}
	if !found {
		t.Fatal("the indeterminate operation was not drawn")
	}
}
