package cluster

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestTargetValidity(t *testing.T) {
	for _, tg := range Targets() {
		if !tg.Valid() {
			t.Errorf("%s is in Targets() but reports itself invalid", tg)
		}
	}
	for _, bogus := range []Target{"", "kv", "etcd", "kvsingle "} {
		if bogus.Valid() {
			t.Errorf("%q was accepted as a target", bogus)
		}
	}
}

func TestSingleIsAlwaysOneNode(t *testing.T) {
	// Three copies of a single-node store are three unrelated databases, and a
	// run against them would be meaningless rather than merely wrong.
	for _, requested := range []int{0, 1, 3, 9} {
		if got := Single.NodeCount(requested); got != 1 {
			t.Errorf("Single.NodeCount(%d) = %d, want 1", requested, got)
		}
	}
}

func TestReplicatedTargetsNeedAtLeastThree(t *testing.T) {
	for _, tg := range []Target{Forward, Split, Sync, Quorum} {
		for _, requested := range []int{-1, 0, 1} {
			if got := tg.NodeCount(requested); got != 3 {
				t.Errorf("%s.NodeCount(%d) = %d, want 3", tg, requested, got)
			}
		}
		if got := tg.NodeCount(5); got != 5 {
			t.Errorf("%s.NodeCount(5) = %d, want 5", tg, got)
		}
	}
}

// peerAddr is a stand-in for the harness's proxy addressing.
func peerAddr(from, to int) string {
	return "proxy-" + string(rune('a'+from)) + "-" + string(rune('a'+to))
}

func TestDefaultConfigForForward(t *testing.T) {
	// Split is Forward with a flag, so it must get the same topology; a missing
	// case here would leave every follower with no leader and the control group
	// would quietly become a cluster of three unrelated stores.
	for _, target := range []Target{Forward, Split} {
		c := &Cluster{target: target, nodes: []*Node{{ID: "n0"}, {ID: "n1"}, {ID: "n2"}}}
		cfg := c.DefaultConfig(peerAddr)
		if cfg[1].Leader != peerAddr(1, 0) {
			t.Errorf("%s node 1 forwards to %q, want %q", target, cfg[1].Leader, peerAddr(1, 0))
		}
	}

	c := &Cluster{target: Forward, nodes: []*Node{{ID: "n0"}, {ID: "n1"}, {ID: "n2"}}}
	cfg := c.DefaultConfig(peerAddr)

	if len(cfg) != 3 {
		t.Fatalf("%d configurations for 3 nodes", len(cfg))
	}
	// An empty leader means "I am the leader". Node 0 leads.
	if cfg[0].Leader != "" {
		t.Errorf("node 0 was told to forward to %q; it is supposed to be the leader", cfg[0].Leader)
	}
	for i := 1; i < 3; i++ {
		if want := peerAddr(i, 0); cfg[i].Leader != want {
			t.Errorf("node %d forwards to %q, want %q", i, cfg[i].Leader, want)
		}
		if len(cfg[i].Peers) != 0 {
			t.Errorf("node %d was given peers; a forwarding follower has only a leader", i)
		}
	}
}

func TestSyncSharesTheQuorumBinaryButNotItsFlags(t *testing.T) {
	// kvsync is kvquorum with one flag flipped. If they ever stop sharing a
	// binary the demonstration silently starts building two copies of the same
	// program, and if the flag is lost the control group becomes a duplicate of
	// the experiment.
	if Sync.binary() != string(Quorum) {
		t.Errorf("Sync.binary() = %q, want %q", Sync.binary(), Quorum)
	}
	if got := Sync.extraArgs(); len(got) != 1 || got[0] != "-sync" {
		t.Errorf("Sync.extraArgs() = %v, want [-sync]", got)
	}
	if Split.binary() != string(Forward) {
		t.Errorf("Split.binary() = %q, want %q", Split.binary(), Forward)
	}
	if got := Split.extraArgs(); len(got) != 1 || got[0] != "-promote" {
		t.Errorf("Split.extraArgs() = %v, want [-promote]", got)
	}
	for _, tg := range []Target{Single, Forward, Quorum} {
		if tg.binary() != string(tg) {
			t.Errorf("%s.binary() = %q", tg, tg.binary())
		}
		if got := tg.extraArgs(); len(got) != 0 {
			t.Errorf("%s.extraArgs() = %v, want none", tg, got)
		}
	}
}

func TestReplicated(t *testing.T) {
	if Single.Replicated() {
		t.Error("a single-node target has no peer hops to proxy")
	}
	for _, tg := range []Target{Forward, Split, Sync, Quorum} {
		if !tg.Replicated() {
			t.Errorf("%s has peers and must have its peer hops proxied", tg)
		}
	}
}

func TestDefaultConfigForQuorum(t *testing.T) {
	for _, target := range []Target{Quorum, Sync} {
		c := &Cluster{target: target, nodes: []*Node{{ID: "n0"}, {ID: "n1"}, {ID: "n2"}}}
		if got := len(c.DefaultConfig(peerAddr)[0].Peers); got != 2 {
			t.Errorf("%s node 0 has %d peers, want 2", target, got)
		}
	}

	c := &Cluster{target: Quorum, nodes: []*Node{{ID: "n0"}, {ID: "n1"}, {ID: "n2"}}}
	cfg := c.DefaultConfig(peerAddr)

	for i := 0; i < 3; i++ {
		if len(cfg[i].Peers) != 2 {
			t.Fatalf("node %d has %d peers, want 2", i, len(cfg[i].Peers))
		}
		for _, p := range cfg[i].Peers {
			if p == peerAddr(i, i) {
				t.Errorf("node %d was told to replicate to itself", i)
			}
		}
		// Every peer address must be the one routed through this node's own
		// proxy, or a partition would cut the wrong direction.
		want := map[string]bool{}
		for j := 0; j < 3; j++ {
			if j != i {
				want[peerAddr(i, j)] = true
			}
		}
		for _, p := range cfg[i].Peers {
			if !want[p] {
				t.Errorf("node %d has peer %q, which is not one of its own proxy addresses", i, p)
			}
		}
	}
}

func TestDefaultConfigForSingleSaysNothing(t *testing.T) {
	c := &Cluster{target: Single, nodes: []*Node{{ID: "n0"}}}
	cfg := c.DefaultConfig(peerAddr)
	if len(cfg) != 1 {
		t.Fatalf("%d configurations for 1 node", len(cfg))
	}
	if cfg[0].Leader != "" || len(cfg[0].Peers) != 0 {
		t.Fatalf("a single node was given a topology: %+v", cfg[0])
	}
}

func TestConfigureRefusesAMismatchedLength(t *testing.T) {
	c := &Cluster{target: Quorum, nodes: []*Node{{ID: "n0"}, {ID: "n1"}}}
	if err := c.Configure(t.Context(), []NodeConfig{{}}); err == nil {
		t.Fatal("Configure accepted one configuration for two nodes; the second would silently stay unconfigured")
	}
}

func TestExeName(t *testing.T) {
	got := exeName("kvsingle")
	if runtime.GOOS == "windows" {
		if got != "kvsingle.exe" {
			t.Fatalf("exeName = %q on windows", got)
		}
		return
	}
	if got != "kvsingle" {
		t.Fatalf("exeName = %q", got)
	}
}

func TestModuleRootFindsTheRepository(t *testing.T) {
	root, err := moduleRoot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("moduleRoot returned %q, which has no go.mod: %v", root, err)
	}
}

func TestModuleRootFailsOutsideAModule(t *testing.T) {
	// A confusing failure here would surface much later as "the binary does not
	// exist", so the error must be raised where the cause is.
	dir := t.TempDir()
	// A temporary directory can sit under a module on some machines; only run
	// the check when it genuinely does not.
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
		t.Skip("the temporary directory is inside a module")
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	if root, err := moduleRoot(); err == nil {
		t.Skipf("the temporary directory is below a module at %s", root)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c := &Cluster{target: Single}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("the second Close returned %v", err)
	}
}

func TestReachableSaysNoForADeadPort(t *testing.T) {
	if Reachable("127.0.0.1:9", 100_000_000) {
		t.Skip("something is listening on the discard port on this machine")
	}
}
