// Package cluster starts the systems under test as real, separate operating
// system processes.
//
// Running the nodes as goroutines inside the test binary would be easier and
// would still use real sockets, but it would quietly weaken the claim: shared
// memory, one scheduler, one crash domain. Separate processes mean a partition
// is genuinely a partition, and that killing a node kills a node.
package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Target names one of the stores this repository ships.
type Target string

// The targets, which between them make the point the repository exists to
// make: consistency and availability under partition are a trade, the trade is
// visible in the histories, and it takes a broken network to see it.
const (
	// Single is one node with one mutex. Linearizable, and unavailable the
	// moment its node is cut off.
	Single Target = "kvsingle"
	// Forward is three nodes that all funnel every operation to a fixed
	// leader. Linearizable, and unavailable to whichever node loses the
	// leader.
	Forward Target = "kvforward"
	// Quorum is three nodes with local reads and best-effort asynchronous
	// replication. Always available, and not linearizable - on a healthy
	// network as much as on a broken one.
	Quorum Target = "kvquorum"

	// Sync replicates synchronously before a write returns, but still replies
	// "ok" when a replica could not be reached. A different way to be wrong
	// from Quorum, and measurably wrong on a healthy network too: compare-and-
	// swap is uncoordinated, and last-writer-wins on a coarse clock lets an
	// earlier write beat a later one that had already returned.
	Sync Target = "kvsync"

	// Split is the bug this repository is named after. It is Forward with one
	// flag: a follower that cannot reach the leader for a few attempts in a row
	// declares itself leader and starts serving from whatever it last saw go
	// past, while still replying "ok".
	//
	// It is the control in the experiment. On a healthy network the leader is
	// always reachable, nothing ever promotes, and it is linearizable. Under a
	// partition the isolated side promotes, both halves serve clients, and they
	// disagree. It is the target that shows the fault injection is what finds
	// the bug, rather than the bug being there for anyone to trip over.
	Split Target = "kvsplit"
)

// Targets lists every target, in the order they should be presented: the two
// correct ones, then the one that is only wrong under partition, then the two
// that are wrong regardless.
func Targets() []Target { return []Target{Single, Forward, Split, Sync, Quorum} }

// binary is the executable a target runs. Some targets are another target's
// program with one flag flipped, so they share a binary.
func (t Target) binary() string {
	switch t {
	case Sync:
		return string(Quorum)
	case Split:
		return string(Forward)
	default:
		return string(t)
	}
}

// extraArgs are the flags that distinguish a target from its binary.
func (t Target) extraArgs() []string {
	switch t {
	case Sync:
		return []string{"-sync"}
	case Split:
		return []string{"-promote"}
	default:
		return nil
	}
}

// Replicated reports whether a target has peers to talk to, and therefore
// whether peer hops need proxying.
func (t Target) Replicated() bool { return t != Single }

// NodeCount is how many processes a target needs. Single is a single node by
// definition; running three copies of it would be three unrelated databases.
func (t Target) NodeCount(requested int) int {
	if t == Single {
		return 1
	}
	if requested < 2 {
		return 3
	}
	return requested
}

// Valid reports whether t is a target this package can start.
func (t Target) Valid() bool {
	for _, c := range Targets() {
		if c == t {
			return true
		}
	}
	return false
}

// Options controls how a cluster is started.
type Options struct {
	Target Target
	// Nodes is how many processes to start. Ignored for Single.
	Nodes int
	// BinDir holds prebuilt binaries. When empty, Start builds them with the
	// go tool into a temporary directory, which needs go on PATH.
	BinDir string
	// Timeout is the per-request timeout the nodes use when they talk to each
	// other. It must be short: a follower that waits a long time for an
	// unreachable leader turns every client operation into a timeout, and the
	// history ends up empty.
	Timeout time.Duration
	// Stderr receives the nodes' logs. Nil discards them.
	Stderr io.Writer
	// StartTimeout bounds how long to wait for a node to announce its address
	// and answer a health check.
	StartTimeout time.Duration
}

// A Node is one running server process.
type Node struct {
	ID   string
	Addr string

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	logDone chan struct{}
}

// A Cluster is a set of running nodes.
type Cluster struct {
	target  Target
	nodes   []*Node
	tempBin string

	closeOnce sync.Once
	closeErr  error
}

// Start launches the cluster. The nodes are bound and answering health checks
// when it returns, but they are NOT yet configured: they do not know about
// each other. That is deliberate - the caller puts fault-injecting proxies in
// front of every hop first, then calls Configure with the proxy addresses, so
// that peer traffic is as breakable as client traffic. A partition that only
// cuts clients off is not a partition.
func Start(ctx context.Context, opt Options) (_ *Cluster, err error) {
	if !opt.Target.Valid() {
		return nil, fmt.Errorf("cluster: unknown target %q (have %v)", opt.Target, Targets())
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 250 * time.Millisecond
	}
	if opt.StartTimeout <= 0 {
		opt.StartTimeout = 20 * time.Second
	}

	// Deliberately not a named return. An earlier version returned the cluster
	// through one, so `return nil, err` set it to nil before this deferred
	// cleanup ran, and every startup failure became a nil dereference that hid
	// the actual error - which was the one thing the caller needed.
	c := &Cluster{target: opt.Target}
	defer func() {
		if err != nil {
			_ = c.Close()
		}
	}()

	binDir := opt.BinDir
	if binDir == "" {
		dir, buildErr := buildBinaries(ctx, opt.Target)
		if buildErr != nil {
			return nil, buildErr
		}
		binDir, c.tempBin = dir, dir
	}
	bin := filepath.Join(binDir, exeName(opt.Target.binary()))
	if _, statErr := os.Stat(bin); statErr != nil {
		return nil, fmt.Errorf("cluster: %s: %w", bin, statErr)
	}

	n := opt.Target.NodeCount(opt.Nodes)
	for i := 0; i < n; i++ {
		node, startErr := startNode(ctx, bin, fmt.Sprintf("n%d", i), opt)
		if startErr != nil {
			return nil, startErr
		}
		c.nodes = append(c.nodes, node)
	}
	return c, nil
}

// exeName adds the extension Windows insists on.
func exeName(base string) string {
	if runtime.GOOS == "windows" {
		return base + ".exe"
	}
	return base
}

// buildBinaries compiles the target into a temporary directory.
func buildBinaries(ctx context.Context, t Target) (string, error) {
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp("", "splitbrain-bin-")
	if err != nil {
		return "", err
	}
	out := filepath.Join(dir, exeName(t.binary()))
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags", "-s -w", "-o", out, "./cmd/"+t.binary())
	cmd.Dir = root
	if combined, buildErr := cmd.CombinedOutput(); buildErr != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("cluster: building %s failed (%w); pass a prebuilt directory instead:\n%s",
			t, buildErr, strings.TrimSpace(string(combined)))
	}
	return dir, nil
}

// moduleRoot walks up from the working directory looking for go.mod.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("cluster: no go.mod above the working directory; run from inside the repository or pass a prebuilt binary directory")
		}
		dir = parent
	}
}

// startNode launches one process and waits until it is answering.
func startNode(ctx context.Context, bin, id string, opt Options) (*Node, error) {
	args := []string{"-addr", "127.0.0.1:0", "-id", id, "-timeout", opt.Timeout.String()}
	args = append(args, opt.Target.extraArgs()...)
	cmd := exec.Command(bin, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("cluster: starting %s: %w", bin, err)
	}

	node := &Node{ID: id, cmd: cmd, stdin: stdin, stdout: stdout, logDone: make(chan struct{})}
	go func() {
		defer close(node.logDone)
		if opt.Stderr == nil {
			_, _ = io.Copy(io.Discard, stderr)
			return
		}
		drainStderr(opt.Stderr, stderr, id)
	}()

	// The first line of stdout is the contract: "listening 127.0.0.1:PORT".
	// Anything else means the binary changed underneath us, and guessing an
	// address would send the whole run at nothing.
	type lineResult struct {
		line string
		err  error
	}
	lines := make(chan lineResult, 1)
	go func() {
		r := bufio.NewReader(stdout)
		line, err := r.ReadString('\n')
		lines <- lineResult{strings.TrimSpace(line), err}
		// Keep draining so the child never blocks on a full pipe.
		_, _ = io.Copy(io.Discard, r)
	}()

	select {
	case lr := <-lines:
		if lr.err != nil && lr.line == "" {
			_ = node.kill()
			return nil, fmt.Errorf("cluster: %s exited before announcing an address: %w", id, lr.err)
		}
		addr, ok := strings.CutPrefix(lr.line, "listening ")
		if !ok {
			_ = node.kill()
			return nil, fmt.Errorf("cluster: %s printed %q, want \"listening <addr>\"", id, lr.line)
		}
		node.Addr = strings.TrimSpace(addr)
	case <-time.After(opt.StartTimeout):
		_ = node.kill()
		return nil, fmt.Errorf("cluster: %s did not announce an address within %s", id, opt.StartTimeout)
	case <-ctx.Done():
		_ = node.kill()
		return nil, ctx.Err()
	}

	if err := waitHealthy(ctx, node.Addr, opt.StartTimeout); err != nil {
		_ = node.kill()
		return nil, fmt.Errorf("cluster: %s: %w", id, err)
	}
	return node, nil
}

// maxLogLine is how much of one log line the drain will hold in order to put a
// node's identifier in front of it. The scanner's own default is 64 KB, which a
// node that logs a whole request body goes past without trying.
const maxLogLine = 1 << 20

// drainStderr copies a node's log stream to dst, a line at a time with the node
// named in front, and does not stop until the stream does.
//
// The fallback copy at the end looks redundant and is not. A scanner that ends
// on an error stops reading, and the node on the other end has not stopped
// writing: its stderr pipe fills, and it blocks for ever in the middle of a log
// call, holding whatever lock it was under. What that looks like from the
// outside is a node that went silent and then stopped answering, with nothing
// pointing at the log line that did it. The prefix is worth giving up to keep
// the pipe emptying; the drain is not.
func drainStderr(dst io.Writer, r io.Reader, id string) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLogLine)
	for sc.Scan() {
		fmt.Fprintf(dst, "[%s] %s\n", id, sc.Text())
	}
	// A clean end of stream reports no error, so this is only the trouble case.
	if err := sc.Err(); err != nil {
		fmt.Fprintf(dst, "[%s] this node's output can no longer be split into lines (%v); the rest is copied through as it arrives\n", id, err)
		_, _ = io.Copy(dst, r)
	}
}

// waitHealthy polls /health until it answers or the deadline passes.
func waitHealthy(ctx context.Context, addr string, within time.Duration) error {
	deadline := time.Now().Add(within)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	var last error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Errorf("health returned %s", resp.Status)
		} else {
			last = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	if last == nil {
		last = fmt.Errorf("no response")
	}
	return fmt.Errorf("never became healthy: %w", last)
}

// Nodes returns the running nodes in start order.
func (c *Cluster) Nodes() []*Node { return c.nodes }

// Addrs returns the nodes' real addresses in start order.
func (c *Cluster) Addrs() []string {
	out := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		out[i] = n.Addr
	}
	return out
}

// Target returns which store is running.
func (c *Cluster) Target() Target { return c.target }

// A NodeConfig is what one node should be told about the others. The addresses
// are whatever the caller wants the node to dial - in a real run they are
// proxy addresses, not the peers' own.
type NodeConfig struct {
	Leader string   `json:"leader,omitempty"`
	Peers  []string `json:"peers,omitempty"`
}

// Configure tells each node about the others. cfg must have one entry per node
// in start order.
func (c *Cluster) Configure(ctx context.Context, cfg []NodeConfig) error {
	if len(cfg) != len(c.nodes) {
		return fmt.Errorf("cluster: %d configurations for %d nodes", len(cfg), len(c.nodes))
	}
	client := &http.Client{Timeout: 2 * time.Second}
	for i, node := range c.nodes {
		body, err := json.Marshal(cfg[i])
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+node.Addr+"/configure", strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("cluster: configuring %s: %w", node.ID, err)
		}
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("cluster: configuring %s: %s: %s", node.ID, resp.Status, strings.TrimSpace(string(payload)))
		}
		var ack struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(payload, &ack); err != nil {
			return fmt.Errorf("cluster: configuring %s: unreadable reply %q", node.ID, string(payload))
		}
		if !ack.OK {
			return fmt.Errorf("cluster: configuring %s: %s", node.ID, ack.Error)
		}
	}
	return nil
}

// DefaultConfig is the usual topology for a target, given the addresses the
// nodes should use to reach each other. peerAddr(from, to) returns the address
// node `from` should dial to reach node `to`, which lets the caller route peer
// traffic through its own proxies.
func (c *Cluster) DefaultConfig(peerAddr func(from, to int) string) []NodeConfig {
	n := len(c.nodes)
	out := make([]NodeConfig, n)
	switch c.target {
	case Single:
		// Nothing to say.
	case Forward, Split:
		// Node 0 leads. Everyone else forwards to it. A fixed leader is not a
		// consensus protocol and is not meant to be: the point is that it is
		// linearizable and that it stops answering when the leader is cut off -
		// unless it has been told to promote itself instead, which is Split.
		for i := 1; i < n; i++ {
			out[i].Leader = peerAddr(i, 0)
		}
	case Quorum, Sync:
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if i != j {
					out[i].Peers = append(out[i].Peers, peerAddr(i, j))
				}
			}
		}
	}
	return out
}

// Close stops every node and removes any binaries this package built. It is
// safe on a nil cluster and safe to call more than once, because it is what
// every failure path calls and a cleanup that panics buries the real error.
func (c *Cluster) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		for _, n := range c.nodes {
			if err := n.stop(); err != nil && c.closeErr == nil {
				c.closeErr = err
			}
		}
		if c.tempBin != "" {
			if err := RemoveBuiltBinaries(c.tempBin); err != nil {
				// Reported as well as returned. Every caller in this repository
				// closes a cluster with `defer c.Close()`, so a returned error
				// alone would be dropped at exactly the moment it mattered, and
				// this is the same choice faultnet makes when it cannot reclaim
				// a port: there is nowhere to return a cleanup failure to, and
				// silence is how it accumulates.
				log.Print(err)
				if c.closeErr == nil {
					c.closeErr = err
				}
			}
		}
	})
	return c.closeErr
}

// removeWindow bounds how long RemoveBuiltBinaries keeps trying.
//
// Windows holds a handle on a running executable image and does not always
// release it the instant Wait returns, which is the instant the cleanup runs.
// The directory is then removable again a moment later - three left behind on
// this machine all deleted on the first attempt hours afterwards - so a single
// attempt turns a transient sharing violation into seven megabytes of
// permanent litter per run.
const removeWindow = 2 * time.Second

// RemoveBuiltBinaries deletes a directory of binaries this package or a caller
// built for one run, retrying for a short while before giving up.
//
// It reports what it could not delete rather than swallowing it. A cleanup
// failure is not worth failing a run over - the verdict is already in - but it
// is worth a line naming the path, because the alternative is a temporary
// directory that grows by one node binary per run and never explains itself.
func RemoveBuiltBinaries(dir string) error {
	deadline := time.Now().Add(removeWindow)
	for {
		err := os.RemoveAll(dir)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster: could not remove the binaries built for this run after %s; delete %s by hand: %w",
				removeWindow, dir, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stop asks a node to exit by closing its stdin, then kills it if it will not.
func (n *Node) stop() error {
	if n.cmd == nil || n.cmd.Process == nil {
		return nil
	}
	if n.stdin != nil {
		_ = n.stdin.Close()
	}
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = n.kill()
		<-done
	}
	if n.stdout != nil {
		_ = n.stdout.Close()
	}
	select {
	case <-n.logDone:
	case <-time.After(time.Second):
	}
	return nil
}

// kill terminates a node without asking.
func (n *Node) kill() error {
	if n.cmd == nil || n.cmd.Process == nil {
		return nil
	}
	return n.cmd.Process.Kill()
}

// PolicyBlocked reports whether err is the operating system refusing to
// execute a binary this package just built.
//
// Windows Smart App Control blocks unsigned executables it has no reputation
// for, and it decides per file hash, so rebuilding the same source can be
// allowed one minute and blocked the next. It is matched by message because
// the failure arrives as a plain fork/exec error with no distinguishable
// errno, and because getting it wrong only costs a slightly worse error
// message. Tests that need a real cluster use this to skip with an
// explanation, rather than reporting a machine policy as a defect in the code.
func PolicyBlocked(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Application Control policy")
}

// Reachable reports whether something is listening on addr right now. It is
// used by tests to confirm a stopped node really released its port.
func Reachable(addr string, within time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, within)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
