package campaign

import (
	"os/exec"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/checker"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/cluster"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/harness"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// These are the only tests in the repository that start real processes and open
// real sockets, so they are skipped under -short. Everything they exercise is
// also exercised by tools/demo, which CI runs separately and at greater length;
// what these add is that `go test ./...` alone still proves the pieces fit
// together.
func requireIntegration(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("starts real processes; skipped under -short")
	}
	// The cluster builds the node binaries with the go tool.
	path, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go is not on PATH, so the node binaries cannot be built")
	}
	return path
}

// mustRun runs a campaign, skipping rather than failing when the machine
// refuses to execute the binary it just built. That is a local policy, not a
// defect, and CI runs these for real on a machine without one.
func mustRun(t *testing.T, spec Spec) Outcome {
	t.Helper()
	out, err := Once(t.Context(), spec)
	if cluster.PolicyBlocked(err) {
		t.Skipf("this machine's Application Control policy blocked the built binary: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func shortSpec(target cluster.Target, faults string, seed int64) Spec {
	return Spec{
		Target: target,
		Nodes:  3,
		Harness: harness.Config{
			Clients:   4,
			Keys:      3,
			Duration:  3 * time.Second,
			MaxOps:    600,
			OpTimeout: 250 * time.Millisecond,
			Faults:    faults,
			Seed:      seed,
			Quiesce:   400 * time.Millisecond,
		},
		Checker: checker.Options{MaxVisits: 2_000_000, Timeout: 60 * time.Second, Minimize: true},
		Model:   model.CASRegister{},
	}
}

func TestSingleNodeStoreIsNeverAccused(t *testing.T) {
	requireIntegration(t)

	// One node with one mutex cannot violate linearizability. If this fails,
	// the defect is in the harness's timestamps or in the checker, and that is
	// worth far more than any verdict about a replicated store.
	out := mustRun(t, shortSpec(cluster.Single, "partition", 1))
	if len(out.Run.History) == 0 {
		t.Fatal("the run recorded no operations at all")
	}
	if out.Check.Verdict != checker.Linearizable {
		t.Fatalf("a single-node store was reported %s: %s", out.Check.Verdict, out.Check.Reason)
	}
	t.Logf("%d operations, %d states, %s", len(out.Run.History), out.Check.Visits, out.Check.Elapsed)
}

func TestFaultsActuallyBreakTheNetwork(t *testing.T) {
	requireIntegration(t)

	// A run that partitions nothing produces a clean verdict and no evidence.
	// This is the gate that stops the whole suite from passing vacuously.
	out := mustRun(t, shortSpec(cluster.Quorum, "partition", 2))
	if out.DroppedBytes() == 0 {
		t.Fatal("the partition schedule dropped zero bytes; nothing was ever cut")
	}
	if out.ForwardedBytes() == 0 {
		t.Fatal("no bytes reached a node; the proxies were not in the path")
	}
}

func TestBlackholedClientsProduceIndeterminateOperations(t *testing.T) {
	requireIntegration(t)

	// The hardest case the checker has to handle is an operation with no
	// answer, so a suite that never produces one is not exercising it. The
	// single-node schedule blackholes the only client link, so this does not
	// depend on which subset a partition happened to pick.
	out := mustRun(t, shortSpec(cluster.Single, "partition", 4))
	_, _, info := out.Run.History.Counts()
	if info == 0 {
		t.Fatal("nothing came back indeterminate although the only link was blackholed mid-run; " +
			"either the client timeout is longer than the cut or the fault never reached a live connection")
	}
	if out.Check.Verdict != checker.Linearizable {
		t.Fatalf("a single-node store was reported %s: %s", out.Check.Verdict, out.Check.Reason)
	}
	t.Logf("%d of %d operations were indeterminate, and the history still linearizes", info, len(out.Run.History))
}

func TestHealthyNetworkStillMovesTraffic(t *testing.T) {
	requireIntegration(t)

	out := mustRun(t, shortSpec(cluster.Quorum, "none", 3))
	if out.DroppedBytes() != 0 {
		t.Fatalf("the control run dropped %d bytes; it is supposed to be a healthy network", out.DroppedBytes())
	}
	if len(out.Run.History) < 50 {
		t.Fatalf("only %d operations on a healthy network in 3 seconds", len(out.Run.History))
	}
}

func TestUnknownTargetIsRefused(t *testing.T) {
	if _, err := Once(t.Context(), Spec{Target: "kvmagic"}); err == nil {
		t.Fatal("an unknown target was accepted")
	}
}
