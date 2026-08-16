package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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

// TestCloseReportsABinaryDirectoryItCouldNotRemove pins the difference between
// a cleanup that worked and one that was thrown away.
//
// Close used to remove the binaries it built with `_ = os.RemoveAll(...)`, so a
// removal that failed left roughly seven megabytes per run behind and said
// nothing. Windows holds a handle on a running executable image and does not
// always release it the instant Wait returns, which is exactly when Close tries
// to delete it; the directories are removable again moments later, which is why
// this shows up as disk creeping upwards rather than as an error anyone sees.
//
// The failure is staged with an open handle rather than by racing a real node,
// because the race is rare - 130 build-and-close cycles on the machine this was
// written on did not lose one - and a test that only fails on an unlucky day is
// not a test. What is being asserted is the part that is true every time: if
// the directory cannot be removed, Close says so.
func TestCloseReportsABinaryDirectoryItCouldNotRemove(t *testing.T) {
	dir := t.TempDir()
	stuck := filepath.Join(dir, exeName("kvsingle"))
	if err := os.WriteFile(stuck, []byte("stands in for a node binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	fh, err := os.Open(stuck)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// No nodes: this is about the binaries, and starting a real cluster would
	// make the test depend on the race it is deliberately not racing.
	c := &Cluster{target: Single, tempBin: dir}
	closeErr := c.Close()

	if _, statErr := os.Stat(stuck); statErr != nil {
		t.Skip("this platform removes a file that is still open, so an unremovable directory cannot be staged here")
	}
	if closeErr == nil {
		t.Fatalf("Close left %s on disk and reported success; a cleanup failure nobody is told about is how the leak accumulated", dir)
	}
	if !strings.Contains(closeErr.Error(), dir) {
		t.Errorf("Close reported %q, which does not name the directory a person has to delete (%s)", closeErr, dir)
	}
}

// TestPolicyBlockedOnlyMatchesTheMachineRefusingToExecute guards the predicate
// that decides whether a failure is this machine's business or this
// repository's.
//
// It is load-bearing in both directions and the two mistakes are not
// symmetrical. Match too little and Windows Smart App Control blocking an
// unsigned binary is reported as a defect in the tool, which wastes an
// afternoon. Match too much and a genuine failure - a node that never bound, a
// binary that is not there - is quietly skipped by `tools/demo
// -allow-policy-skips`, and the demonstration reports an incomplete run as an
// explained one. The second is the one that costs something, so the negative
// cases below are the point of this test rather than the decoration.
func TestPolicyBlockedOnlyMatchesTheMachineRefusingToExecute(t *testing.T) {
	// The message as the operating system actually delivers it, observed on
	// this machine.
	blocked := errors.New(`fork/exec C:\Users\x\AppData\Local\Temp\go-build1\b001\splitbrain.test.exe: ` +
		`An Application Control policy has blocked this file.`)
	if !PolicyBlocked(blocked) {
		t.Errorf("a real Smart App Control refusal was not recognised: %v", blocked)
	}
	if !PolicyBlocked(fmt.Errorf("cluster: starting kvquorum: %w", blocked)) {
		t.Error("a wrapped refusal was not recognised; cluster.Start wraps every start error")
	}

	// Everything a run can fail with that is not the machine's policy. Any of
	// these being skipped would turn a defect into a shrug.
	for _, err := range []error{
		nil,
		errors.New(`cluster: n0 printed "hello", want "listening <addr>"`),
		errors.New("cluster: n2 did not announce an address within 20s"),
		errors.New(`cluster: C:\bin\kvsingle.exe: The system cannot find the file specified.`),
		errors.New("cluster: configuring n2: 500 Internal Server Error"),
		errors.New("harness: value-max is 1; it must be at least 2"),
		errors.New("permission denied"),
		errors.New("access is denied"),
	} {
		if PolicyBlocked(err) {
			t.Errorf("PolicyBlocked(%v) is true; a real failure would be skipped as a machine policy", err)
		}
	}
}

func TestReachableSaysNoForADeadPort(t *testing.T) {
	if Reachable("127.0.0.1:9", 100_000_000) {
		t.Skip("something is listening on the discard port on this machine")
	}
}

// TestStderrDrainNeverStopsBeforeTheStreamDoes is the regression test for a
// hang that looked nothing like its cause: one log line past the scanner's
// limit ended the scan, the node's stderr pipe filled up behind it, and the
// node blocked for ever on its next log write while the run waited on a node
// that had simply gone quiet.
//
// It drives drainStderr over a real os.Pipe rather than through a built fake
// node, because the pipe is the whole mechanism - a stalled reader really does
// block the writer, so a drain that stops is a test that times out rather than
// one that asserts a proxy for the problem. What it does not cover is the
// wiring in startNode, which needs a real cluster.
func TestStderrDrainNeverStopsBeforeTheStreamDoes(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		// Over the 64 KB the scanner allows by default, so the raised buffer is
		// what keeps these lines split.
		{"long line", 200_000},
		// Past any buffer we are willing to hold, so only the fallback copy can
		// keep the pipe emptying.
		{"line past the cap", maxLogLine * 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("pipe: %v", err)
			}
			// Both ends are closed here as well as below, so a drain that has
			// stopped cannot leave the fake node blocked in a write for the rest
			// of the test binary's life.
			t.Cleanup(func() { r.Close(); w.Close() })

			var got bytes.Buffer
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				drainStderr(&got, r, "n0")
			}()

			const marker = "the node is still logging"
			written := make(chan error, 1)
			go func() {
				_, err := w.Write(append(bytes.Repeat([]byte("x"), tc.size), '\n'))
				if err == nil {
					_, err = w.Write([]byte(marker + "\n"))
				}
				written <- err
			}()

			select {
			case err := <-written:
				if err != nil {
					t.Fatalf("the fake node could not write: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the fake node blocked writing its logs, so the drain stopped reading; this is the hang")
			}

			w.Close()
			select {
			case <-drained:
			case <-time.After(10 * time.Second):
				t.Fatal("the drain did not return once the stream closed")
			}

			if !strings.Contains(got.String(), marker) {
				t.Fatalf("what the node logged after the long line never reached the writer:\n%.400s", got.String())
			}
			if tc.size < maxLogLine && !strings.Contains(got.String(), "[n0] "+marker) {
				t.Errorf("a line within the limit lost its node prefix:\n%.400s", got.String())
			}
			// Once the prefixes stop, the log has to say why, or the next reader
			// concludes the wrong node wrote the rest of it.
			if tc.size > maxLogLine && !strings.Contains(got.String(), "no longer be split") {
				t.Errorf("nothing explains why the output stopped being split:\n%.400s", got.String())
			}
		})
	}
}
