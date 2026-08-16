package kv

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// listeningLine is the contract between every binary and the harness. The
// harness reads exactly this from stdout to learn which port the kernel chose,
// so a stray stdout line anywhere in these programs would send every later
// request to the wrong place.
var listeningLine = regexp.MustCompile(`^listening 127\.0\.0\.1:(\d+)$`)

// binaries are the three commands, with the configuration each needs before it
// will answer a /kv request on its own.
var binaries = []struct {
	name string
	cfg  Config
}{
	{"kvsingle", Config{}},
	// An empty leader address makes the node its own leader, which is what
	// the harness sends to exactly one of the three. Pointing a node at its
	// own address would make it forward to itself for ever.
	{"kvforward", Config{Leader: strptr("")}},
	// An empty peer list is a node that replicates to nobody, which is enough
	// to answer locally.
	{"kvquorum", Config{Peers: &[]string{}}},
}

// A running binary, with everything the test needs to drive it and to check
// what it said.
type node struct {
	cmd    *exec.Cmd
	addr   string
	stdin  io.WriteCloser
	stdout *lineSink
	stderr *lineSink

	// done is closed once the process has exited, and exitErr is written
	// before it closes. A channel that carries the result instead would be
	// readable exactly once, so whichever of stop and the cleanup ran second
	// would block until its own timeout and then kill a process that had
	// already gone.
	done    chan struct{}
	exitErr error
}

// A lineSink collects everything a process writes to one stream, and hands out
// complete lines as they arrive.
//
// It is attached with cmd.Stdout rather than cmd.StdoutPipe on purpose.
// StdoutPipe's reader is closed by Wait, so output still sitting in the pipe
// when the process exits can be lost - which is exactly the case a test for
// "nothing else may go to stdout" has to catch. Assigning an io.Writer makes
// exec copy the stream itself and makes Wait block until the copy is finished,
// so lines() is complete once the process has gone.
type lineSink struct {
	mu      sync.Mutex
	partial []byte
	lines   []string
	ch      chan string
}

func newLineSink() *lineSink { return &lineSink{ch: make(chan string, 256)} }

func (s *lineSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partial = append(s.partial, p...)
	for {
		i := bytes.IndexByte(s.partial, '\n')
		if i < 0 {
			return len(p), nil
		}
		line := strings.TrimRight(string(s.partial[:i]), "\r")
		s.partial = s.partial[i+1:]
		s.lines = append(s.lines, line)
		select {
		case s.ch <- line:
		default: // The buffer is generous; a test that stops reading is fine.
		}
	}
}

// all returns every complete line seen so far, plus any unterminated tail, so
// a stray write with no newline is still visible.
func (s *lineSink) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.lines...)
	if len(s.partial) > 0 {
		out = append(out, string(s.partial))
	}
	return out
}

func (s *lineSink) text() string { return strings.Join(s.all(), "\n") }

// Built binaries are shared by every test in this file. Linking three commands
// costs several seconds each on some platforms, and doing it once per test
// dominated the package's runtime.
var (
	buildMu   sync.Mutex
	buildDir  string
	buildPath = map[string]string{}
)

// TestMain provides a temporary directory that outlives an individual test, so
// the binaries can be built once and reused.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "splitbrain-kv")
	if err != nil {
		panic(err)
	}
	buildDir = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// build compiles one command, once. It builds the named package only, never
// the whole module, so that unrelated work elsewhere in the repository cannot
// turn into a failure here.
func build(t *testing.T, name string) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go is not on PATH, so the binaries cannot be built")
	}
	buildMu.Lock()
	defer buildMu.Unlock()
	if out, ok := buildPath[name]; ok {
		return out
	}
	out := filepath.Join(buildDir, name)
	if runtime.GOOS == "windows" {
		out += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", out, "./cmd/"+name)
	cmd.Dir = filepath.Join("..", "..")
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building %s: %v\n%s", name, err, combined)
	}
	buildPath[name] = out
	return out
}

// applicationControlBlocked is Windows system error 4551, "An Application
// Control policy has blocked this file".
const applicationControlBlocked = syscall.Errno(4551)

// skipIfBlockedByPolicy skips when the operating system refused to execute a
// freshly built binary at all.
//
// A locked-down Windows machine blocks unsigned executables by hash, and for a
// hash its reputation service has not seen the verdict is effectively
// arbitrary: the same source rebuilt with different linker flags runs. That is
// the machine declining to run the test rather than the test failing, and
// nothing inside the process can work around it.
//
// The skip is deliberately narrow - one errno, one platform, and only when
// exec itself failed - because a broader one would quietly delete every
// subprocess test in this file. Linux CI, where the race detector also runs,
// executes all of them for real.
func skipIfBlockedByPolicy(t *testing.T, err error) {
	t.Helper()
	if runtime.GOOS != "windows" {
		return
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && errno == applicationControlBlocked {
		t.Skipf("this machine's Application Control policy refused to execute the freshly built "+
			"binary (Windows error %d); it is a local device restriction, not a defect. Original "+
			"error: %v", uint64(errno), err)
	}
}

// start runs bin with the exact argument vector the cluster launcher uses and
// waits for it to announce its address.
func start(t *testing.T, bin string, args ...string) *node {
	t.Helper()
	cmd := exec.Command(bin, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}
	stdout, stderr := newLineSink(), newLineSink()
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		skipIfBlockedByPolicy(t, err)
		t.Fatalf("start: %v", err)
	}

	n := &node{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
		done:   make(chan struct{}),
	}
	go func() {
		n.exitErr = cmd.Wait()
		close(n.done)
	}()
	t.Cleanup(func() {
		stdin.Close()
		select {
		case <-n.done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			<-n.done
		}
	})

	line := n.readLine(t, 20*time.Second)
	if !listeningLine.MatchString(line) {
		t.Fatalf("first stdout line = %q, want %q\nstderr:\n%s", line, "listening 127.0.0.1:PORT", stderr.text())
	}
	n.addr = strings.TrimPrefix(line, "listening ")
	return n
}

// readLine waits for the next line of stdout, or fails.
func (n *node) readLine(t *testing.T, within time.Duration) string {
	t.Helper()
	select {
	case line := <-n.stdout.ch:
		return line
	case <-time.After(within):
		t.Fatalf("no line on stdout within %v\nstderr:\n%s", within, n.stderr.text())
		return ""
	}
}

// stop closes stdin and expects the process to exit cleanly. Closing stdin is
// how the harness stops a node, because Windows has no SIGTERM that a parent
// can usefully send.
func (n *node) stop(t *testing.T, within time.Duration) {
	t.Helper()
	n.stdin.Close()
	select {
	case <-n.done:
		if n.exitErr != nil {
			t.Errorf("exited with %v\nstderr:\n%s", n.exitErr, n.stderr.text())
		}
	case <-time.After(within):
		n.cmd.Process.Kill()
		t.Fatalf("did not exit within %v of stdin closing; a node that lingers holds its port "+
			"and poisons every later run", within)
	}
}

// The contract test. Every binary takes the launcher's argument vector,
// announces one parseable address, accepts configuration over HTTP and then
// answers a real operation on the address it announced.
func TestBinariesAnnounceConfigureAndServe(t *testing.T) {
	for _, b := range binaries {
		t.Run(b.name, func(t *testing.T) {
			bin := build(t, b.name)
			// Exactly the vector the cluster launcher uses. A binary missing
			// one of these flags exits with status 2 before announcing
			// anything, which is a confusing way to find out.
			n := start(t, bin, "-addr", "127.0.0.1:0", "-id", "n1", "-timeout", "250ms")

			c := NewClient(n.addr, 5*time.Second)
			ctx := context.Background()

			id, err := c.Health(ctx)
			if err != nil {
				t.Fatalf("health: %v\nstderr:\n%s", err, n.stderr.text())
			}
			if id != "n1" {
				t.Errorf("health id = %q, want %q", id, "n1")
			}

			if err := c.Configure(ctx, b.cfg); err != nil {
				t.Fatalf("configure: %v\nstderr:\n%s", err, n.stderr.text())
			}

			if op := c.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 11}); op.Outcome != history.OK {
				t.Fatalf("write = %s (%s)\nstderr:\n%s", op.Outcome, op.Err, n.stderr.text())
			}
			if op := c.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != 11 {
				t.Errorf("read = %s/%d, want ok/11", op.Outcome, op.Observed)
			}
			if op := c.Do(ctx, testClock, 0, Request{Kind: history.CAS, Key: "x", From: 11, To: 12}); op.Outcome != history.OK || !op.Swapped {
				t.Errorf("cas = %s/swapped=%t, want ok/true", op.Outcome, op.Swapped)
			}

			n.stop(t, 10*time.Second)

			// Nothing but the announcement may ever reach stdout. The process
			// has exited and exec has finished copying its output, so
			// everything it ever wrote is here to be counted.
			if got := n.stdout.all(); len(got) != 1 {
				t.Errorf("stdout carried %d lines, want exactly the announcement:\n%s",
					len(got), strings.Join(got, "\n"))
			}
			if len(n.stderr.all()) == 0 {
				t.Error("nothing was logged to stderr; the logger is not wired up")
			}
		})
	}
}

// The opt-in flags have to parse, because Go's flag package exits with status
// 2 on an unknown one and the only symptom is a node that never announced an
// address.
func TestBinariesAcceptTheirOptionalFlags(t *testing.T) {
	for _, tc := range []struct {
		binary string
		args   []string
	}{
		{"kvforward", []string{"-leader", "", "-promote", "-promote-after", "2"}},
		{"kvquorum", []string{"-peers", "", "-sync"}},
	} {
		t.Run(tc.binary, func(t *testing.T) {
			bin := build(t, tc.binary)
			args := append([]string{"-addr", "127.0.0.1:0", "-id", "n1", "-timeout", "250ms"}, tc.args...)
			n := start(t, bin, args...)
			if _, err := NewClient(n.addr, 5*time.Second).Health(context.Background()); err != nil {
				t.Errorf("health: %v\nstderr:\n%s", err, n.stderr.text())
			}
			n.stop(t, 10*time.Second)
		})
	}
}

// Closing stdin is the stop signal the harness relies on, so it gets its own
// assertion separate from the round-trip test above.
func TestBinariesExitOnStdinClose(t *testing.T) {
	for _, b := range binaries {
		t.Run(b.name, func(t *testing.T) {
			bin := build(t, b.name)
			n := start(t, bin, "-addr", "127.0.0.1:0", "-id", "n1", "-timeout", "250ms")
			n.stop(t, 10*time.Second)
		})
	}
}

// SIGTERM is the other stop signal. It is gated on the platform rather than
// skipped quietly, because Windows genuinely has no way for a parent to send
// it - os.Process.Signal refuses anything but Kill there - and the harness
// uses stdin instead. On Linux, where CI runs, this is the path that matters.
func TestBinariesExitOnSIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows has no SIGTERM a parent can send; TestBinariesExitOnStdinClose covers stopping there")
	}
	for _, b := range binaries {
		t.Run(b.name, func(t *testing.T) {
			bin := build(t, b.name)
			n := start(t, bin, "-addr", "127.0.0.1:0", "-id", "n1", "-timeout", "250ms")
			if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatalf("signal: %v", err)
			}
			select {
			case <-n.done:
				if n.exitErr != nil {
					t.Errorf("exited with %v\nstderr:\n%s", n.exitErr, n.stderr.text())
				}
			case <-time.After(10 * time.Second):
				n.cmd.Process.Kill()
				t.Fatal("did not exit within 10s of SIGTERM")
			}
		})
	}
}

// The port must really be released, or the harness's next run fails to bind.
func TestBinaryReleasesItsPort(t *testing.T) {
	bin := build(t, "kvsingle")

	first := start(t, bin, "-addr", "127.0.0.1:0", "-id", "n1", "-timeout", "250ms")
	addr := first.addr
	first.stop(t, 10*time.Second)

	second := start(t, bin, "-addr", addr, "-id", "n2", "-timeout", "250ms")
	if second.addr != addr {
		t.Errorf("second node bound %s, want the first node's %s", second.addr, addr)
	}
	if _, err := NewClient(second.addr, 5*time.Second).Health(context.Background()); err != nil {
		t.Errorf("health on the reused port: %v", err)
	}
	second.stop(t, 10*time.Second)
}

// Three real processes, wired together after the fact exactly as the harness
// does it, showing the forwarding path over real sockets rather than through
// an in-process client.
func TestForwardClusterOverRealProcesses(t *testing.T) {
	bin := build(t, "kvforward")
	ctx := context.Background()

	leader := start(t, bin, "-addr", "127.0.0.1:0", "-id", "n1", "-timeout", "250ms")
	f1 := start(t, bin, "-addr", "127.0.0.1:0", "-id", "n2", "-timeout", "250ms")
	f2 := start(t, bin, "-addr", "127.0.0.1:0", "-id", "n3", "-timeout", "250ms")

	lc := NewClient(leader.addr, 5*time.Second)
	c1 := NewClient(f1.addr, 5*time.Second)
	c2 := NewClient(f2.addr, 5*time.Second)

	if err := lc.Configure(ctx, Config{Leader: strptr("")}); err != nil {
		t.Fatalf("configure leader: %v", err)
	}
	for i, c := range []*Client{c1, c2} {
		if err := c.Configure(ctx, Config{Leader: strptr(leader.addr)}); err != nil {
			t.Fatalf("configure follower %d: %v", i, err)
		}
	}

	if op := c1.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 5}); op.Outcome != history.OK {
		t.Fatalf("write via follower 1 = %s (%s)\n%s", op.Outcome, op.Err, f1.stderr.text())
	}
	// Written through one follower, read back through the other: both really
	// are talking to the same leader.
	if op := c2.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != 5 {
		t.Errorf("read via follower 2 = %s/%d, want ok/5", op.Outcome, op.Observed)
	}

	// Stop the leader and the followers must stop answering rather than start
	// inventing values.
	leader.stop(t, 10*time.Second)
	for i, c := range []*Client{c1, c2} {
		op := c.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
		if op.Outcome == history.OK {
			t.Errorf("follower %d answered %d with the leader gone", i, op.Observed)
		}
	}

	f1.stop(t, 10*time.Second)
	f2.stop(t, 10*time.Second)
}

// The AP corner, over real processes: two nodes that cannot reach each other
// disagree, and the disagreement is a real-time violation.
func TestQuorumClusterDisagreesOverRealProcesses(t *testing.T) {
	bin := build(t, "kvquorum")
	ctx := context.Background()
	dead := closedPort(t)

	a := start(t, bin, "-addr", "127.0.0.1:0", "-id", "na", "-timeout", "250ms", "-peers", dead)
	b := start(t, bin, "-addr", "127.0.0.1:0", "-id", "nb", "-timeout", "250ms", "-peers", dead)

	ca := NewClient(a.addr, 5*time.Second)
	cb := NewClient(b.addr, 5*time.Second)
	origin := testClock

	w := ca.Do(ctx, origin, 0, Request{Kind: history.Write, Key: "x", Value: 7})
	if w.Outcome != history.OK {
		t.Fatalf("write to a = %s (%s)", w.Outcome, w.Err)
	}
	time.Sleep(10 * time.Millisecond) // clear the platform clock's granularity
	r := cb.Do(ctx, origin, 1, Request{Kind: history.Read, Key: "x"})
	if r.Outcome != history.OK {
		t.Fatalf("read from b = %s (%s); this store is supposed to stay available", r.Outcome, r.Err)
	}
	if r.Observed == 7 {
		t.Fatal("b returned a's value; the two processes did not disagree")
	}
	if w.Complete > r.Invoke {
		t.Fatalf("the write did not finish before the read began (%d..%d then %d..)", w.Invoke, w.Complete, r.Invoke)
	}
	if err := (history.History{w, r}).Validate(); err != nil {
		t.Errorf("the pair is not a valid history: %v", err)
	}

	a.stop(t, 10*time.Second)
	b.stop(t, 10*time.Second)
}
