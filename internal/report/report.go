// Package report renders a history and a verdict as a single self-contained
// HTML page.
//
// A counterexample printed as text is correct and almost unreadable: the reason
// a history is impossible lives in the way intervals overlap, and prose is a bad
// medium for that. Drawn on a time axis it is usually obvious within seconds -
// two bars that do not overlap, and the later one returning a value the earlier
// one ruled out.
//
// The page has no external assets, no scripts fetched from anywhere and no
// dependencies. It is one file that opens in any browser, including offline,
// which is what makes it worth attaching to anything.
package report

import (
	"fmt"
	"html/template"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/checker"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/faultnet"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// Input is everything the page needs.
type Input struct {
	Title string

	// Verdict is the checker's answer.
	Verdict checker.Result

	// History is the whole run. Only a window of it is drawn.
	History history.History

	// Faults is the timeline that was applied, drawn underneath the operations
	// so a reader can see which partition the violation happened in.
	Faults []faultnet.Event

	// ProcessNode maps a process to the node it was talking to. A violation is
	// far easier to read when you can see that the two disagreeing clients were
	// on opposite sides of a cut.
	ProcessNode map[int]string

	// MaxOps caps how many operations are drawn. Zero uses a sensible default.
	// A run of several thousand operations makes an unreadable page and a very
	// large file, so the window is the operations nearest the violation.
	MaxOps int

	// Summary is a line of free text about the run, shown under the verdict.
	Summary string
}

const defaultMaxOps = 250

// row is one drawn operation.
type row struct {
	Y           int
	X, W        int
	Label       string
	Detail      string
	Class       string
	Culprit     bool
	Pending     bool
	TextX       int
	TextAnchor  string
	LaneLabel   string
	ResultLabel string
}

type faultBand struct {
	X, W  int
	Label string
	Class string
}

type pageData struct {
	Title    string
	Verdict  string
	Class    string
	Reason   string
	Summary  string
	Stats    []stat
	Rows     []row
	Bands    []faultBand
	Ticks    []tick
	Width    int
	Height   int
	PlotLeft int
	Drawn    int
	Total    int
	Note     string
	Key      string
	Legend   []legendItem
}

type stat struct{ Name, Value string }
type tick struct {
	X     int
	Label string
}
type legendItem struct{ Class, Label string }

const (
	plotLeft   = 148
	plotRight  = 32
	pageWidth  = 1180
	rowHeight  = 19
	topPad     = 34
	bandHeight = 16
)

// Write renders the page.
func (in Input) Write(w io.Writer) error {
	data, err := in.build()
	if err != nil {
		return err
	}
	return page.Execute(w, data)
}

func (in Input) build() (pageData, error) {
	maxOps := in.MaxOps
	if maxOps <= 0 {
		maxOps = defaultMaxOps
	}

	// Draw the counterexample when there is one: it is the smallest window that
	// still contains the violation, so it is exactly what a reader wants. Fall
	// back to the failing key, and then to the whole history.
	shown := in.Verdict.Ops
	key := in.Verdict.Key
	note := "the minimal failing truncation: every operation up to the moment the violation became unavoidable"
	if len(shown) == 0 {
		if key != "" {
			shown = in.History.ByKey()[key]
			note = "every operation on the key the checker reported"
		} else {
			shown = in.History
			note = "the whole recorded history"
		}
	}
	total := len(shown)
	if total == 0 {
		shown = in.History
		total = len(shown)
		note = "the whole recorded history"
	}
	shown = append(history.History(nil), shown...)
	shown.SortByInvoke()

	// Keep the operations nearest the end, which is where the violation is.
	if len(shown) > maxOps {
		shown = shown[len(shown)-maxOps:]
		note += fmt.Sprintf("; showing the last %d of %d for legibility", maxOps, total)
	}

	culprits := map[opKey]bool{}
	if in.Verdict.Culprit != nil {
		culprits[keyOf(*in.Verdict.Culprit)] = true
	}

	lo, hi := span(shown)
	if hi <= lo {
		hi = lo + 1
	}
	plotW := pageWidth - plotLeft - plotRight
	scale := func(t int64) int {
		if t == history.Pending {
			return plotLeft + plotW
		}
		if t <= lo {
			return plotLeft
		}
		if t >= hi {
			return plotLeft + plotW
		}
		return plotLeft + int(float64(t-lo)/float64(hi-lo)*float64(plotW))
	}

	d := pageData{
		Title:    orDefault(in.Title, "Splitbrain report"),
		Verdict:  strings.ToUpper(in.Verdict.Verdict.String()),
		Class:    verdictClass(in.Verdict.Verdict),
		Reason:   in.Verdict.Reason,
		Summary:  in.Summary,
		Width:    pageWidth,
		PlotLeft: plotLeft,
		Drawn:    len(shown),
		Total:    total,
		Note:     note,
		Key:      key,
		Legend: []legendItem{
			{"ok", "completed"},
			{"info", "indeterminate: no answer came back"},
			{"fail", "definitely did not happen"},
			{"culprit", "the operation the search could not place"},
			{"band", "network cut"},
		},
	}

	for i, op := range shown {
		x0, x1 := scale(op.Invoke), scale(op.Complete)
		if x1-x0 < 3 {
			x1 = x0 + 3
		}
		r := row{
			Y:           topPad + i*rowHeight,
			X:           x0,
			W:           x1 - x0,
			Class:       outcomeClass(op.Outcome),
			Culprit:     culprits[keyOf(op)],
			Pending:     op.Outcome == history.Info,
			Label:       describe(op),
			ResultLabel: result(op),
			LaneLabel:   lane(op, in.ProcessNode),
			Detail:      detail(op, in.ProcessNode),
		}
		if r.Culprit {
			r.Class += " culprit"
		}
		// Put the result label wherever there is room for it.
		if x1+180 < pageWidth-plotRight {
			r.TextX, r.TextAnchor = x1+6, "start"
		} else {
			r.TextX, r.TextAnchor = x0-6, "end"
		}
		d.Rows = append(d.Rows, r)
	}

	d.Bands = bands(in.Faults, lo, hi, scale)
	d.Ticks = ticks(lo, hi, scale)
	d.Height = topPad + len(shown)*rowHeight + 28

	ok, fail, info := in.History.Counts()
	d.Stats = []stat{
		{"operations recorded", fmt.Sprint(len(in.History))},
		{"completed", fmt.Sprint(ok)},
		{"indeterminate", fmt.Sprint(info)},
		{"definitely failed", fmt.Sprint(fail)},
		{"keys", fmt.Sprint(len(in.History.Keys()))},
		{"model states explored", fmt.Sprint(in.Verdict.Visits)},
		{"checking time", in.Verdict.Elapsed.Round(time.Millisecond).String()},
		{"fault events", fmt.Sprint(len(in.Faults))},
	}
	return d, nil
}

type opKey struct {
	proc   int
	invoke int64
	key    string
}

func keyOf(op history.Op) opKey { return opKey{op.Process, op.Invoke, op.Key} }

func span(ops history.History) (lo, hi int64) {
	if len(ops) == 0 {
		return 0, 1
	}
	lo = ops[0].Invoke
	for _, op := range ops {
		if op.Invoke < lo {
			lo = op.Invoke
		}
		if op.Complete != history.Pending && op.Complete > hi {
			hi = op.Complete
		}
		if op.Invoke > hi {
			hi = op.Invoke
		}
	}
	return lo, hi
}

// bands draws the intervals during which at least one link was cut. The exact
// link is not shown: what a reader needs is whether the violation happened
// during a cut or after the heal, and a band per link would bury that.
func bands(events []faultnet.Event, lo, hi int64, scale func(int64) int) []faultBand {
	if len(events) == 0 {
		return nil
	}
	sorted := append([]faultnet.Event(nil), events...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].At < sorted[j].At })

	down := map[string]bool{}
	var out []faultBand
	var openAt time.Duration
	open := false
	countDown := func() int {
		n := 0
		for _, v := range down {
			if v {
				n++
			}
		}
		return n
	}
	for _, e := range sorted {
		down[e.Link] = e.Fault != faultnet.Pass
		switch {
		case !open && countDown() > 0:
			open, openAt = true, e.At
		case open && countDown() == 0:
			out = append(out, band(openAt, e.At, lo, hi, scale))
			open = false
		}
	}
	if open {
		out = append(out, band(openAt, time.Duration(hi-lo), lo, hi, scale))
	}
	return out
}

func band(from, to time.Duration, lo, hi int64, scale func(int64) int) faultBand {
	x0 := scale(lo + int64(from))
	x1 := scale(lo + int64(to))
	if x1-x0 < 2 {
		x1 = x0 + 2
	}
	return faultBand{X: x0, W: x1 - x0, Label: fmt.Sprintf("cut %s", from.Round(time.Millisecond)), Class: "band"}
}

func ticks(lo, hi int64, scale func(int64) int) []tick {
	var out []tick
	const n = 8
	for i := 0; i <= n; i++ {
		t := lo + (hi-lo)*int64(i)/n
		out = append(out, tick{X: scale(t), Label: time.Duration(t - lo).Round(time.Millisecond).String()})
	}
	return out
}

func outcomeClass(o history.Outcome) string {
	switch o {
	case history.OK:
		return "ok"
	case history.Fail:
		return "fail"
	default:
		return "info"
	}
}

func verdictClass(v checker.Verdict) string {
	switch v {
	case checker.Linearizable:
		return "good"
	case checker.NotLinearizable:
		return "bad"
	default:
		return "unsure"
	}
}

func describe(op history.Op) string {
	switch op.Kind {
	case history.Read:
		return fmt.Sprintf("read %s", op.Key)
	case history.Write:
		return fmt.Sprintf("write %s = %d", op.Key, op.Value)
	case history.CAS:
		return fmt.Sprintf("cas %s: %d → %d", op.Key, op.From, op.To)
	}
	return op.Kind.String()
}

func result(op history.Op) string {
	switch {
	case op.Outcome == history.Info:
		return "no answer"
	case op.Outcome == history.Fail:
		return "refused"
	case op.Kind == history.Read:
		return fmt.Sprintf("= %d", op.Observed)
	case op.Kind == history.CAS && op.Swapped:
		return "swapped"
	case op.Kind == history.CAS:
		return "not swapped"
	default:
		return "ok"
	}
}

func lane(op history.Op, nodes map[int]string) string {
	if n, ok := nodes[op.Process]; ok {
		return fmt.Sprintf("p%d @ %s", op.Process, n)
	}
	return fmt.Sprintf("p%d", op.Process)
}

func detail(op history.Op, nodes map[int]string) string {
	end := time.Duration(op.Complete).Round(time.Microsecond).String()
	if op.Outcome == history.Info {
		end = "never"
	}
	s := fmt.Sprintf("%s  %s  invoked %s, answered %s, %s",
		lane(op, nodes), describe(op),
		time.Duration(op.Invoke).Round(time.Microsecond), end, result(op))
	if op.Err != "" {
		s += "  (" + op.Err + ")"
	}
	return s
}

func orDefault(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}

var page = template.Must(template.New("report").Parse(pageHTML))

const pageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}}</title>
<style>
:root {
  color-scheme: light dark;
  --bg: #ffffff; --fg: #16191d; --muted: #5c6570; --rule: #d8dde3;
  --ok: #2f6f4f; --okf: #dcefe4;
  --info: #8a6d1f; --infof: #f6ecd2;
  --fail: #6b6b6b; --failf: #e7e7e7;
  --bad: #a32b2b; --good: #2f6f4f; --unsure: #8a6d1f;
  --band: rgba(163, 43, 43, 0.10);
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #14171a; --fg: #e6e9ec; --muted: #98a2ad; --rule: #2b3138;
    --ok: #79c79b; --okf: #1d3128;
    --info: #d8b45a; --infof: #33290f;
    --fail: #9aa1a8; --failf: #262a2e;
    --bad: #f08b8b; --good: #79c79b; --unsure: #d8b45a;
    --band: rgba(240, 139, 139, 0.12);
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 28px 22px 64px; background: var(--bg); color: var(--fg);
  font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
}
main { max-width: 1180px; margin: 0 auto; }
h1 { font-size: 19px; margin: 0 0 4px; font-weight: 620; letter-spacing: -0.01em; }
.sub { color: var(--muted); margin: 0 0 22px; }
.verdict { font: 600 26px/1.2 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; letter-spacing: -0.02em; }
.verdict.good { color: var(--good); }
.verdict.bad { color: var(--bad); }
.verdict.unsure { color: var(--unsure); }
.reason { margin: 6px 0 0; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px; }
.card { border: 1px solid var(--rule); border-radius: 10px; padding: 18px 20px; margin: 0 0 20px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 14px 20px; margin-top: 16px; }
.stat b { display: block; font: 600 17px/1.2 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.stat span { color: var(--muted); font-size: 12px; }
.legend { display: flex; flex-wrap: wrap; gap: 16px; margin: 0 0 10px; font-size: 12px; color: var(--muted); }
.legend i { display: inline-block; width: 22px; height: 9px; border-radius: 2px; margin-right: 6px; vertical-align: 1px; }
.legend i.ok { background: var(--okf); border: 1px solid var(--ok); }
.legend i.info { background: var(--infof); border: 1px solid var(--info); }
.legend i.fail { background: var(--failf); border: 1px solid var(--fail); }
.legend i.culprit { background: var(--bad); }
.legend i.band { background: var(--band); border: 1px solid var(--rule); }
.scroll { overflow-x: auto; }
svg { display: block; }
svg text { font: 11px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; fill: var(--fg); }
svg text.lane { fill: var(--muted); }
svg text.axis { fill: var(--muted); font-size: 10px; }
rect.ok { fill: var(--okf); stroke: var(--ok); }
rect.info { fill: var(--infof); stroke: var(--info); stroke-dasharray: 3 2; }
rect.fail { fill: var(--failf); stroke: var(--fail); }
rect.culprit { stroke: var(--bad); stroke-width: 2; }
rect.band { fill: var(--band); stroke: none; }
line.grid { stroke: var(--rule); stroke-width: 1; }
.note { color: var(--muted); font-size: 12px; margin: 10px 0 0; }
footer { color: var(--muted); font-size: 12px; margin-top: 26px; }
</style>
</head>
<body>
<main>
  <h1>{{.Title}}</h1>
  <p class="sub">A recorded history, checked for linearizability.</p>

  <div class="card">
    <div class="verdict {{.Class}}">{{.Verdict}}</div>
    {{if .Reason}}<p class="reason">{{.Reason}}</p>{{end}}
    {{if .Summary}}<p class="note">{{.Summary}}</p>{{end}}
    <div class="stats">
      {{range .Stats}}<div class="stat"><b>{{.Value}}</b><span>{{.Name}}</span></div>{{end}}
    </div>
  </div>

  <div class="card">
    <div class="legend">
      {{range .Legend}}<span><i class="{{.Class}}"></i>{{.Label}}</span>{{end}}
    </div>
    <div class="scroll">
      <svg width="{{.Width}}" height="{{.Height}}" viewBox="0 0 {{.Width}} {{.Height}}" role="img"
           aria-label="operations on a time axis">
        {{range .Bands}}<rect class="{{.Class}}" x="{{.X}}" y="16" width="{{.W}}" height="{{$.Height}}"><title>{{.Label}}</title></rect>{{end}}
        {{range .Ticks}}<line class="grid" x1="{{.X}}" y1="18" x2="{{.X}}" y2="{{$.Height}}"/><text class="axis" x="{{.X}}" y="12" text-anchor="middle">{{.Label}}</text>{{end}}
        {{range .Rows}}
        <text class="lane" x="{{$.PlotLeft}}" y="{{.Y}}" dx="-8" text-anchor="end" dy="10">{{.LaneLabel}}</text>
        <rect class="{{.Class}}" x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="13" rx="2"><title>{{.Detail}}</title></rect>
        <text x="{{.TextX}}" y="{{.Y}}" dy="10" text-anchor="{{.TextAnchor}}">{{.Label}} {{.ResultLabel}}</text>
        {{end}}
      </svg>
    </div>
    <p class="note">{{.Note}}{{if .Key}} (key <code>{{.Key}}</code>){{end}}. Hover a bar for the full operation.</p>
  </div>

  <footer>Generated by Splitbrain. Bars run from the moment the client sent the request to the moment it
  learned the answer; a dashed bar reaching the right edge never got one.</footer>
</main>
</body>
</html>
`
