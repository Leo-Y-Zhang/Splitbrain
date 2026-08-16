// Command demo is the repository's headline claim, executable.
//
// It builds the four stores, runs each of them through the same seeded
// partition schedules over real TCP as separate operating-system processes,
// and checks every history. Then it asserts what the repository claims:
//
//   - the single-node store is never accused (a violation there would be a bug
//     in this tool, not in the store);
//   - the leader-forwarding store is never accused either, even though the
//     partitions are real and it loses availability during them;
//   - the synchronously replicated store, which replies "ok" even when a
//     replica was unreachable, is caught;
//   - so is the asynchronously replicated one.
//
// It then runs the two replicated stores again on a healthy network. That is
// the control, and it is the only part of this that says whether the fault
// injection is doing any work: asynchronous replication is wrong with or
// without a partition, but the synchronous one should behave until something
// cuts the network.
//
// It also asserts that the runs did any work at all. A harness that partitions
// nothing and issues no operations produces clean verdicts and proves less than
// nothing, so the byte counters and operation counts are gates in their own
// right.
//
// CI runs this on a machine that is not the author's, from a clean checkout,
// on every push.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/campaign"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/checker"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/cluster"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/harness"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// expectation is what the demonstration asserts about a target.
type expectation int

const (
	// mustBeClean: every seed must come back linearizable. Any violation is
	// evidence against this tool.
	mustBeClean expectation = iota
	// mustBeCaught: at least one seed must find a violation.
	mustBeCaught
)

type subject struct {
	target cluster.Target
	nodes  int
	expect expectation
	why    string
}

func main() {
	seeds := flag.Int("seeds", 5, "how many seeds to run per target")
	duration := flag.Duration("duration", 6*time.Second, "length of each run")
	clients := flag.Int("clients", 6, "concurrent clients")
	keys := flag.Int("keys", 4, "independent registers")
	maxOps := flag.Int("max-ops", 2500, "cap on operations per run")
	out := flag.String("out", "", "directory to write histories and a summary into")
	control := flag.Bool("control", true, "also run the replicated stores with a healthy network, for comparison")
	allowSkips := flag.Bool("allow-policy-skips", false,
		"carry on when the machine refuses to execute a binary this builds, instead of failing; "+
			"Windows Smart App Control does this to unsigned executables. Never set in CI, and every skip is reported.")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *seeds, *duration, *clients, *keys, *maxOps, *out, *control, *allowSkips); err != nil {
		fmt.Fprintf(os.Stderr, "\ndemo: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, seeds int, duration time.Duration, clients, keys, maxOps int, outDir string, control, allowSkips bool) error {
	binDir, err := buildAll(ctx)
	if err != nil {
		return err
	}
	defer os.RemoveAll(binDir)

	if outDir != "" {
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return err
		}
	}

	subjects := []subject{
		{cluster.Single, 1, mustBeClean, "one node, one mutex: linearizable by construction, and unavailable when it is cut off"},
		{cluster.Forward, 3, mustBeClean, "three nodes funnelling everything to a fixed leader: linearizable, and unavailable to whoever loses the leader"},
		{cluster.Split, 3, mustBeCaught, "the same, except a follower that loses the leader promotes itself and serves from what it last saw: fine until the network breaks, then two leaders"},
		{cluster.Sync, 3, mustBeCaught, "three nodes replicating synchronously but replying ok even when a replica was unreachable"},
		{cluster.Quorum, 3, mustBeCaught, "three nodes with local reads and asynchronous replication: always available, and wrong with or without a partition"},
	}

	fmt.Printf("splitbrain demonstration: %d seeds per target, %s each, %d clients, %d keys, real TCP between separate processes\n",
		seeds, duration, clients, keys)
	fmt.Printf("go %s on %s/%s\n\n", strings.TrimPrefix(runtime.Version(), "go"), runtime.GOOS, runtime.GOARCH)

	var failures, skipped []string
	summary := &strings.Builder{}

	for _, s := range subjects {
		fmt.Printf("%s - %s\n", s.target, s.why)
		res, err := sweep(ctx, s.target, s.nodes, seeds, duration, clients, keys, maxOps, binDir, "partition", outDir)
		if cluster.PolicyBlocked(err) && allowSkips {
			note := fmt.Sprintf("%s partition: SKIPPED, this machine refused to execute the binary (%v)", s.target, err)
			fmt.Printf("  %s\n\n", note)
			fmt.Fprintln(summary, note)
			skipped = append(skipped, string(s.target))
			continue
		}
		if err != nil {
			return err
		}
		fmt.Print(res.table())
		fmt.Printf("  %s\n\n", res.line())
		fmt.Fprintf(summary, "%s partition: %s\n", s.target, res.line())

		switch s.expect {
		case mustBeClean:
			if res.notLinearizable > 0 {
				failures = append(failures, fmt.Sprintf(
					"%s was accused on %d of %d seeds; it is correct by construction, so this is a defect in the checker or the harness, not in the store",
					s.target, res.notLinearizable, res.total))
			}
			if res.unknown > 0 {
				failures = append(failures, fmt.Sprintf(
					"%s exhausted the search budget on %d of %d seeds; an unknown verdict is not a pass",
					s.target, res.unknown, res.total))
			}
		case mustBeCaught:
			if res.notLinearizable == 0 {
				failures = append(failures, fmt.Sprintf(
					"%s was never caught across %d seeds; either the fault injection stopped working or the checker stopped checking",
					s.target, res.total))
			}
		}

		// A run that partitioned nothing and moved nothing is not evidence.
		if res.dropped == 0 {
			failures = append(failures, fmt.Sprintf("%s: the partition schedule dropped zero bytes across every seed", s.target))
		}
		if res.forwarded == 0 {
			failures = append(failures, fmt.Sprintf("%s: no bytes ever reached a node", s.target))
		}
		if res.ops < int64(seeds*20) {
			failures = append(failures, fmt.Sprintf("%s: only %d operations across %d seeds, which is too few to conclude anything", s.target, res.ops, seeds))
		}
	}

	if control {
		// The control group, and the only part of this that says whether the
		// fault injection is doing any work rather than merely running.
		//
		// kvsplit is the one that carries the argument: the same store, the
		// same workload and the same checker as the run above, differing only
		// in whether the network was broken. If it comes back clean here and
		// caught there, the partition is the cause. The other two are measured
		// rather than asserted - they are wrong with or without a network to
		// blame, and saying so is more useful than pretending otherwise.
		controls := []struct {
			target   cluster.Target
			mustPass bool
		}{
			{cluster.Split, true},
			{cluster.Sync, false},
			{cluster.Quorum, false},
		}
		for _, c := range controls {
			fmt.Printf("control: %s with a healthy network\n", c.target)
			res, err := sweep(ctx, c.target, 3, seeds, duration, clients, keys, maxOps, binDir, "none", outDir)
			if cluster.PolicyBlocked(err) && allowSkips {
				note := fmt.Sprintf("%s healthy: SKIPPED, this machine refused to execute the binary", c.target)
				fmt.Printf("  %s\n\n", note)
				fmt.Fprintln(summary, note)
				skipped = append(skipped, string(c.target)+" (control)")
				continue
			}
			if err != nil {
				return err
			}
			fmt.Print(res.table())
			fmt.Printf("  %s\n\n", res.line())
			fmt.Fprintf(summary, "%s healthy: %s\n", c.target, res.line())

			if res.dropped != 0 {
				failures = append(failures, fmt.Sprintf(
					"the healthy control for %s dropped %d bytes; it is not a control if the network was broken", c.target, res.dropped))
			}
			if c.mustPass && res.linearizable != res.total {
				failures = append(failures, fmt.Sprintf(
					"%s was caught on %d of %d seeds with a healthy network; it is meant to be correct until something cuts the network, "+
						"so the comparison with its partitioned run no longer shows that the partition is what found the bug",
					c.target, res.total-res.linearizable, res.total))
			}
		}
	}

	if outDir != "" {
		if err := os.WriteFile(filepath.Join(outDir, "summary.txt"), []byte(summary.String()), 0o644); err != nil {
			return err
		}
	}

	if len(failures) > 0 {
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "FAILED: %s\n", f)
		}
		return fmt.Errorf("%d assertion(s) failed", len(failures))
	}

	if len(skipped) > 0 {
		// Loudly, and last, so it cannot be mistaken for a complete run. A
		// partial demonstration that reads like a full one is worse than a
		// failure.
		fmt.Printf("INCOMPLETE: %d subject(s) were skipped because this machine refused to execute their binary: %s\n",
			len(skipped), strings.Join(skipped, ", "))
		fmt.Println("Every subject that did run met its expectation. Run this where nothing blocks unsigned binaries for the whole claim.")
		return nil
	}

	fmt.Println("every assertion held: the correct stores were never accused, the incorrect ones were caught, and the network really was broken.")
	return nil
}

// sweepResult accumulates one target's seeds.
type sweepResult struct {
	target                                        cluster.Target
	faults                                        string
	total, linearizable, notLinearizable, unknown int
	ops, dropped, forwarded                       int64
	rows                                          []string
	firstViolation                                string
}

func (r sweepResult) line() string {
	s := fmt.Sprintf("seeds=%d linearizable=%d not-linearizable=%d unknown=%d operations=%d forwarded=%dB dropped=%dB",
		r.total, r.linearizable, r.notLinearizable, r.unknown, r.ops, r.forwarded, r.dropped)
	if r.firstViolation != "" {
		s += "\n  first violation: " + r.firstViolation
	}
	return s
}

func (r sweepResult) table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "  %-6s %-7s %-6s %-18s %-9s %s\n", "seed", "ops", "info", "verdict", "states", "detail")
	for _, row := range r.rows {
		b.WriteString(row)
	}
	return b.String()
}

func sweep(ctx context.Context, target cluster.Target, nodes, seeds int, duration time.Duration,
	clients, keys, maxOps int, binDir, faults, outDir string) (sweepResult, error) {

	res := sweepResult{target: target, faults: faults, total: seeds}
	for seed := 0; seed < seeds; seed++ {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		out, err := campaign.Once(ctx, campaign.Spec{
			Target: target,
			Nodes:  nodes,
			BinDir: binDir,
			Harness: harness.Config{
				Clients:  clients,
				Keys:     keys,
				Duration: duration,
				MaxOps:   maxOps,
				Faults:   faults,
				Seed:     int64(seed),
			},
			Checker: checker.Options{MaxVisits: 3_000_000, Timeout: 90 * time.Second, Minimize: true},
			Model:   model.CASRegister{},
		})
		if err != nil {
			return res, fmt.Errorf("%s seed %d: %w", target, seed, err)
		}

		_, _, info := out.Run.History.Counts()
		res.ops += int64(len(out.Run.History))
		res.dropped += out.DroppedBytes()
		res.forwarded += out.ForwardedBytes()

		detail := ""
		switch out.Check.Verdict {
		case checker.Linearizable:
			res.linearizable++
		case checker.NotLinearizable:
			res.notLinearizable++
			detail = trunc(out.Check.Reason, 64)
			if res.firstViolation == "" {
				res.firstViolation = fmt.Sprintf("%s seed %d, %s", target, seed, out.Check.Reason)
			}
		default:
			res.unknown++
			detail = trunc(out.Check.Reason, 64)
		}

		res.rows = append(res.rows, fmt.Sprintf("  %-6d %-7d %-6d %-18s %-9d %s\n",
			seed, len(out.Run.History), info, out.Check.Verdict, out.Check.Visits, detail))

		if outDir != "" {
			name := fmt.Sprintf("%s-%s-seed%d.jsonl", target, faults, seed)
			fh, err := os.Create(filepath.Join(outDir, name))
			if err != nil {
				return res, err
			}
			err = out.Run.History.Save(fh)
			fh.Close()
			if err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

// buildAll compiles the node binaries once, so that starting a cluster per seed
// costs a process spawn rather than a compile.
func buildAll(ctx context.Context) (string, error) {
	dir, err := os.MkdirTemp("", "splitbrain-demo-")
	if err != nil {
		return "", err
	}
	// Everything under cmd/ in one command. Two of the targets are the same
	// program with a flag flipped, so building per target would compile it
	// twice and then have to know which name to give it.
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags", "-s -w", "-o", dir, "./cmd/...")
	if combined, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("building the stores: %w\n%s", err, strings.TrimSpace(string(combined)))
	}
	return dir, nil
}

func trunc(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
