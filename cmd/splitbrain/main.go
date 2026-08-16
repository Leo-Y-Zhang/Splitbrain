// Command splitbrain drives a real key-value cluster through a seeded network
// partition schedule over real TCP, records what the clients saw, and
// machine-checks the result for linearizability.
//
// Exit status is the verdict: 0 when the run matched what was expected, 1 when
// it did not, and 2 when something went wrong before a verdict existed. That
// makes it usable as a gate rather than only as a report.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/campaign"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/checker"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/cluster"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/harness"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/report"
)

const usage = `splitbrain - find linearizability violations in a networked key-value store

usage:
  splitbrain run       [flags]        drive a cluster through faults and check the result
  splitbrain check     <history.jsonl> [flags]   check a recorded history (flags may go either side)
  splitbrain sweep     [flags]        run many seeds and summarise
  splitbrain schedule  [flags]        print a fault timeline without running anything

exit status:
  0  the verdict matched -expect
  1  it did not
  2  the run could not produce a verdict

run "splitbrain <command> -h" for the flags of one command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(ctx, os.Args[2:])
	case "check":
		err = cmdCheck(os.Args[2:])
	case "sweep":
		err = cmdSweep(ctx, os.Args[2:])
	case "schedule":
		err = cmdSchedule(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "splitbrain: unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}

	if err != nil {
		var mismatch *expectationFailed
		if errors.As(err, &mismatch) {
			fmt.Fprintf(os.Stderr, "\n%s\n", mismatch.Error())
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "splitbrain: %v\n", err)
		os.Exit(2)
	}
}

// expectationFailed separates "the tool worked and the answer was not the one
// you asked for" from "the tool broke". Conflating them makes the exit status
// useless in a pipeline.
type expectationFailed struct{ msg string }

func (e *expectationFailed) Error() string { return e.msg }

// runFlags is the shared configuration of `run` and `sweep`.
type runFlags struct {
	target    string
	nodes     int
	clients   int
	keys      int
	duration  time.Duration
	maxOps    int
	opTimeout time.Duration
	thinkMax  time.Duration
	valueMax  int
	faults    string
	quiesce   time.Duration
	keepAlive bool
	binDir    string
	nodeTmo   time.Duration

	modelName string
	maxVisits int
	checkTmo  time.Duration
	expect    string
	verbose   bool
}

func (f *runFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.target, "target", "kvquorum", "which store to test: kvsingle, kvforward, kvsync or kvquorum")
	fs.IntVar(&f.nodes, "nodes", 3, "how many node processes to start (kvsingle is always 1)")
	fs.IntVar(&f.clients, "clients", 6, "concurrent client processes, spread round-robin across the nodes")
	fs.IntVar(&f.keys, "keys", 4, "independent registers the clients share")
	fs.DurationVar(&f.duration, "duration", 8*time.Second, "how long to run the load and the fault schedule")
	fs.IntVar(&f.maxOps, "max-ops", 4000, "cap on total operations; 0 for no cap")
	fs.DurationVar(&f.opTimeout, "op-timeout", 400*time.Millisecond, "per-operation client timeout")
	fs.DurationVar(&f.thinkMax, "think", 8*time.Millisecond, "upper bound on the pause between one client's operations")
	fs.IntVar(&f.valueMax, "value-max", 1_000_000, "largest value clients write")
	fs.StringVar(&f.faults, "faults", "partition", "fault schedule: none, partition, refuse, flaky or chaos")
	fs.DurationVar(&f.quiesce, "quiesce", 750*time.Millisecond, "settling time after healing, before the final reads")
	fs.BoolVar(&f.keepAlive, "keep-alive", false, "reuse client connections between operations")
	fs.StringVar(&f.binDir, "bin-dir", "", "directory of prebuilt node binaries; empty means build them with the go tool")
	fs.DurationVar(&f.nodeTmo, "node-timeout", 250*time.Millisecond, "timeout the nodes use when talking to each other")

	fs.StringVar(&f.modelName, "model", "cas-register", "sequential model to check against")
	fs.IntVar(&f.maxVisits, "max-visits", 2_000_000, "search budget per key; 0 for no limit")
	fs.DurationVar(&f.checkTmo, "check-timeout", 60*time.Second, "wall-clock budget for the whole check")
	fs.StringVar(&f.expect, "expect", "linearizable", "expected verdict: linearizable, not-linearizable or any")
	fs.BoolVar(&f.verbose, "v", false, "print the fault timeline and per-link traffic")
}

func (f *runFlags) harnessConfig(seed int64) harness.Config {
	return harness.Config{
		Clients:   f.clients,
		Keys:      f.keys,
		Duration:  f.duration,
		MaxOps:    f.maxOps,
		OpTimeout: f.opTimeout,
		ThinkMax:  f.thinkMax,
		ValueMax:  f.valueMax,
		Faults:    f.faults,
		Seed:      seed,
		Quiesce:   f.quiesce,
		KeepAlive: f.keepAlive,
	}
}

func (f *runFlags) checkerOptions() checker.Options {
	return checker.Options{MaxVisits: f.maxVisits, Timeout: f.checkTmo, Minimize: true}
}

// oneRun starts a cluster, runs the load and checks the history.
func oneRun(ctx context.Context, f *runFlags, seed int64, quiet bool) (*harness.Result, checker.Result, error) {
	target := cluster.Target(f.target)
	if !target.Valid() {
		return nil, checker.Result{}, fmt.Errorf("unknown target %q (have kvsingle, kvforward, kvsync, kvquorum)", f.target)
	}
	m, err := model.ByName(f.modelName)
	if err != nil {
		return nil, checker.Result{}, err
	}

	// Declared as the interface, not as *os.File. A nil *os.File stored in an
	// io.Writer is not a nil interface, and the receiving code's nil check
	// would sail straight past it.
	var nodeLogs io.Writer
	if f.verbose {
		nodeLogs = os.Stderr
	}

	out, err := campaign.Once(ctx, campaign.Spec{
		Target:      target,
		Nodes:       f.nodes,
		BinDir:      f.binDir,
		NodeTimeout: f.nodeTmo,
		Harness:     f.harnessConfig(seed),
		Checker:     f.checkerOptions(),
		Model:       m,
		NodeStderr:  nodeLogs,
	})
	if err != nil {
		return nil, checker.Result{}, err
	}

	if !quiet {
		fmt.Printf("cluster: %d %s process(es) on %s\n", len(out.Addrs), target, strings.Join(out.Addrs, ", "))
		fmt.Printf("network: %d links, %d planned fault events\n", len(out.Links), len(out.Run.Schedule.Events()))
		if f.verbose {
			fmt.Print(indent(out.Run.Schedule.String(), "  "))
			fmt.Println("  per-link traffic:")
			names := make([]string, 0, len(out.Run.LinkStats))
			for n := range out.Run.LinkStats {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, n := range names {
				s := out.Run.LinkStats[n]
				fmt.Printf("    %-8s accepted=%d refused=%d reset=%d fwd=%dB back=%dB dropped=%dB\n",
					n, s.Accepted, s.Refused, s.ResetConns, s.BytesForward, s.BytesBack, s.BytesDropped)
			}
		}
		fmt.Printf("run: %s\n", out.Run.Summary())
	}

	return out.Run, out.Check, nil
}

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	f := &runFlags{}
	f.register(fs)
	out := fs.String("out", "", "write the history here as JSON Lines")
	reportPath := fs.String("report", "", "write a self-contained HTML report here")
	counterPath := fs.String("counterexample", "", "write the minimal failing truncation here as JSON Lines")
	seed := fs.Int64("seed", 1, "seed for the fault schedule and the operation generator")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Printf("splitbrain run: target=%s nodes=%d clients=%d keys=%d faults=%s seed=%d duration=%s\n",
		f.target, cluster.Target(f.target).NodeCount(f.nodes), f.clients, f.keys, f.faults, *seed, f.duration)

	res, verdict, err := oneRun(ctx, f, *seed, false)
	if err != nil {
		return err
	}

	if *out != "" {
		if err := writeHistory(*out, res.History); err != nil {
			return err
		}
		fmt.Printf("history: %d operations written to %s\n", len(res.History), *out)
	}
	if *counterPath != "" && len(verdict.Ops) > 0 {
		// The truncation is itself a valid history: every operation still in
		// flight at the cut is recorded as indeterminate, and any operation a
		// process issued afterwards was invoked after the cut and is gone. So
		// it can be checked again on its own, which is what makes it a
		// reproduction rather than a screenshot.
		if err := writeHistory(*counterPath, verdict.Ops); err != nil {
			return err
		}
		fmt.Printf("counterexample: %d operations written to %s\n", len(verdict.Ops), *counterPath)
	}
	if *reportPath != "" {
		in := report.Input{
			Title:       fmt.Sprintf("%s under %s, seed %d", f.target, f.faults, *seed),
			Verdict:     verdict,
			History:     res.History,
			Faults:      res.Applied,
			ProcessNode: res.ProcessNode,
			Summary:     res.Summary(),
		}
		if err := writeReport(*reportPath, in); err != nil {
			return err
		}
		fmt.Printf("report: %s\n", *reportPath)
	}

	printVerdict(verdict, res)
	return checkExpectation(f.expect, verdict.Verdict)
}

func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	modelName := fs.String("model", "cas-register", "sequential model to check against")
	maxVisits := fs.Int("max-visits", 2_000_000, "search budget per key; 0 for no limit")
	timeout := fs.Duration("timeout", 60*time.Second, "wall-clock budget for the whole check")
	expect := fs.String("expect", "any", "expected verdict: linearizable, not-linearizable or any")
	reportPath := fs.String("report", "", "write a self-contained HTML report here")
	files, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(files) != 1 {
		return fmt.Errorf("check needs exactly one history file, got %d", len(files))
	}
	path := files[0]

	m, err := model.ByName(*modelName)
	if err != nil {
		return err
	}

	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	h, err := history.Load(fh)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	ok, fail, info := h.Counts()
	fmt.Printf("splitbrain check: %s, %s across %s (%d ok, %d failed, %d indeterminate), model=%s\n",
		filepath.Base(path), plural(len(h), "operation"), plural(len(h.Keys()), "key"), ok, fail, info, m.Name())

	verdict, err := checker.Check(h, m, checker.Options{MaxVisits: *maxVisits, Timeout: *timeout, Minimize: true})
	if err != nil {
		return err
	}
	if *reportPath != "" {
		in := report.Input{
			Title:   filepath.Base(path),
			Verdict: verdict,
			History: h,
			Summary: fmt.Sprintf("%d operations across %d keys, checked against the %s model",
				len(h), len(h.Keys()), m.Name()),
		}
		if err := writeReport(*reportPath, in); err != nil {
			return err
		}
		fmt.Printf("report: %s\n", *reportPath)
	}

	printVerdict(verdict, nil)
	return checkExpectation(*expect, verdict.Verdict)
}

// writeReport renders the HTML page.
func writeReport(path string, in report.Input) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	return in.Write(fh)
}

func cmdSweep(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sweep", flag.ExitOnError)
	f := &runFlags{}
	f.register(fs)
	seedsArg := fs.String("seeds", "0-9", "inclusive seed range, for example 0-49")
	stopEarly := fs.Bool("stop-on-mismatch", false, "stop at the first seed that contradicts -expect")
	if err := fs.Parse(args); err != nil {
		return err
	}
	lo, hi, err := parseRange(*seedsArg)
	if err != nil {
		return err
	}

	fmt.Printf("splitbrain sweep: target=%s seeds %d..%d faults=%s clients=%d keys=%d duration=%s expect=%s\n",
		f.target, lo, hi, f.faults, f.clients, f.keys, f.duration, f.expect)
	fmt.Printf("%-6s %-7s %-7s %-6s %-18s %-9s %s\n", "seed", "ops", "info", "keys", "verdict", "states", "detail")

	var lin, notLin, unknown, mismatches int
	for seed := lo; seed <= hi; seed++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, verdict, err := oneRun(ctx, f, int64(seed), true)
		if err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
		_, _, info := res.History.Counts()
		detail := ""
		if verdict.Verdict != checker.Linearizable {
			detail = verdict.Reason
			if len(detail) > 70 {
				detail = detail[:67] + "..."
			}
		}
		fmt.Printf("%-6d %-7d %-7d %-6d %-18s %-9d %s\n",
			seed, len(res.History), info, len(res.History.Keys()), verdict.Verdict, verdict.Visits, detail)

		switch verdict.Verdict {
		case checker.Linearizable:
			lin++
		case checker.NotLinearizable:
			notLin++
		default:
			unknown++
		}
		if err := checkExpectation(f.expect, verdict.Verdict); err != nil {
			mismatches++
			if *stopEarly {
				printVerdict(verdict, res)
				return err
			}
		}
	}

	total := hi - lo + 1
	fmt.Printf("\nseeds=%d target=%s faults=%s linearizable=%d not-linearizable=%d unknown=%d mismatches=%d\n",
		total, f.target, f.faults, lin, notLin, unknown, mismatches)
	if mismatches > 0 {
		return &expectationFailed{fmt.Sprintf("%d of %d seeds did not match -expect %s", mismatches, total, f.expect)}
	}
	return nil
}

func cmdSchedule(args []string) error {
	fs := flag.NewFlagSet("schedule", flag.ExitOnError)
	nodes := fs.Int("nodes", 3, "how many nodes the timeline is for")
	faults := fs.String("faults", "partition", "fault schedule kind")
	seed := fs.Int64("seed", 1, "seed")
	duration := fs.Duration("duration", 8*time.Second, "run length")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Proxies to nowhere: the timeline depends only on the link names, and
	// nothing is ever dialled through them.
	addrs := make([]string, *nodes)
	for i := range addrs {
		addrs[i] = "127.0.0.1:9"
	}
	topo, err := harness.BuildTopology(addrs, *nodes > 1)
	if err != nil {
		return err
	}
	defer topo.Close()

	sched, err := harness.BuildSchedule(topo, harness.Config{Faults: *faults, Seed: *seed, Duration: *duration})
	if err != nil {
		return err
	}
	fmt.Printf("splitbrain schedule: nodes=%d faults=%s seed=%d duration=%s links=%s\n",
		*nodes, *faults, *seed, *duration, strings.Join(topo.LinkNames(), ","))
	fmt.Print(sched.String())
	fmt.Printf("%d events\n", len(sched.Events()))
	return nil
}

// printVerdict renders a checker result. res may be nil when checking a file.
func printVerdict(r checker.Result, run *harness.Result) {
	keys := make([]string, 0, len(r.PerKey))
	for k := range r.PerKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		st := r.PerKey[k]
		fmt.Printf("  %-6s %5d ops  %-18s %8d states  %s\n",
			k, st.Ops, st.Verdict, st.Visits, st.Elapsed.Round(time.Millisecond))
	}

	fmt.Printf("verdict: %s (%d states in %s)\n", strings.ToUpper(r.Verdict.String()), r.Visits, r.Elapsed.Round(time.Millisecond))
	if r.Reason != "" {
		fmt.Printf("  %s\n", r.Reason)
	}
	if len(r.Ops) == 0 {
		return
	}

	// The counterexample is a truncation in time, so a violation late in a long
	// run keeps every clean operation before it. Printing all of that buries
	// the interesting end of it, and the HTML report is the right place to read
	// the whole thing.
	const maxLines = 24
	shown := r.Ops
	elided := 0
	if len(shown) > maxLines {
		elided = len(shown) - maxLines
		shown = shown[elided:]
	}

	fmt.Printf("  minimal counterexample, %d operations:\n", len(r.Ops))
	if elided > 0 {
		fmt.Printf("    ... %d earlier operations, all consistent; use -report for the whole picture\n", elided)
	}
	for _, op := range shown {
		node := ""
		if run != nil {
			if n, ok := run.ProcessNode[op.Process]; ok {
				node = "@" + n
			}
		}
		fmt.Printf("    %s\n", renderOp(op, node))
	}
}

// renderOp writes one operation as a line a person can follow.
func renderOp(op history.Op, node string) string {
	when := fmt.Sprintf("[%8s %8s]", dur(op.Invoke), completion(op))
	who := fmt.Sprintf("p%d%s", op.Process, node)

	var what string
	switch op.Kind {
	case history.Read:
		what = fmt.Sprintf("read  %s", op.Key)
	case history.Write:
		what = fmt.Sprintf("write %s = %d", op.Key, op.Value)
	case history.CAS:
		what = fmt.Sprintf("cas   %s: %d -> %d", op.Key, op.From, op.To)
	}

	var got string
	switch {
	case op.Outcome == history.Info:
		got = "indeterminate: " + op.Err
	case op.Outcome == history.Fail:
		got = "failed: " + op.Err
	case op.Kind == history.Read:
		got = fmt.Sprintf("= %d", op.Observed)
	case op.Kind == history.CAS && op.Swapped:
		got = "swapped"
	case op.Kind == history.CAS:
		got = "not swapped"
	default:
		got = "ok"
	}
	return fmt.Sprintf("%s %-10s %-26s %s", when, who, what, got)
}

func dur(ns int64) string {
	return time.Duration(ns).Round(time.Millisecond).String()
}

func completion(op history.Op) string {
	if op.Outcome == history.Info {
		return "-"
	}
	return dur(op.Complete)
}

// checkExpectation turns a verdict into an exit status.
func checkExpectation(expect string, got checker.Verdict) error {
	want := strings.ToLower(strings.TrimSpace(expect))
	switch want {
	case "", "any":
		return nil
	case "linearizable":
		if got == checker.Linearizable {
			return nil
		}
	case "not-linearizable", "not_linearizable", "violation":
		if got == checker.NotLinearizable {
			return nil
		}
	default:
		return fmt.Errorf("unknown -expect %q (have linearizable, not-linearizable, any)", expect)
	}
	return &expectationFailed{fmt.Sprintf("expected %s, got %s", want, got)}
}

func writeHistory(path string, h history.History) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	return h.Save(fh)
}

// parseInterleaved parses flags that may appear before, after or among the
// positional arguments, and returns the positional ones.
//
// Go's flag package stops at the first non-flag argument, so
// `check history.jsonl -expect linearizable` silently treats the expectation
// as a second file rather than as a flag. Every command-line tool that takes
// one file and some options hits this, and the failure is quiet enough to
// waste a genuinely surprising amount of time.
// plural renders a count with its noun, so a report never says "1 keys".
func plural(n int, word string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, word)
	}
	return fmt.Sprintf("%d %ss", n, word)
}

func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return nil, err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		positional = append(positional, rest[0])
		rest = rest[1:]
	}
}

func parseRange(s string) (int, int, error) {
	lo, hi, ok := strings.Cut(s, "-")
	if !ok {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		return n, n, err
	}
	a, err := strconv.Atoi(strings.TrimSpace(lo))
	if err != nil {
		return 0, 0, fmt.Errorf("bad seed range %q: %w", s, err)
	}
	b, err := strconv.Atoi(strings.TrimSpace(hi))
	if err != nil {
		return 0, 0, fmt.Errorf("bad seed range %q: %w", s, err)
	}
	if b < a {
		return 0, 0, fmt.Errorf("bad seed range %q: %d is less than %d", s, b, a)
	}
	return a, b, nil
}

func indent(s, prefix string) string {
	if s == "" {
		return s
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n") + "\n"
}
