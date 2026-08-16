package faultnet

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Timing assertions here only ever pin a lower bound the test itself controls
// (a delay we asked for, a deadline we set). Upper bounds are deliberately
// generous, because these run on shared CI runners where a scheduler stall of
// a few hundred milliseconds is normal and a tight ceiling would fail for
// reasons that have nothing to do with the proxy.

// echoServer starts a loopback echo server for the duration of the test and
// returns its address. It writes back every byte it receives, so a test can
// tell a faithful proxy from a lossy one by comparing the bytes it gets home.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := map[net.Conn]struct{}{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[c] = struct{}{}
			mu.Unlock()
			// Safe to Add from here: this goroutine is itself counted, so
			// the counter cannot reach zero underneath us.
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					mu.Lock()
					delete(conns, c)
					mu.Unlock()
					c.Close()
				}()
				io.Copy(c, c)
			}()
		}
	}()

	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

// quietServer accepts connections and holds them without reading, and without
// starting a goroutine per connection. It exists so a goroutine count can be
// attributed to the link under test rather than to the thing it is proxying
// to: one accept goroutine, in the baseline, and nothing else.
func quietServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("quiet listen: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()

	t.Cleanup(func() {
		ln.Close()
		<-done
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
	})
	return ln.Addr().String()
}

func newTestLink(t *testing.T, target string) *Link {
	t.Helper()
	l, err := NewLink("test", target)
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func dialLink(t *testing.T, l *Link) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", l.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", l.Addr(), err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// roundTrip writes msg and reads the same number of bytes back.
func roundTrip(c net.Conn, msg string, timeout time.Duration) (string, error) {
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return "", err
	}
	if _, err := c.Write([]byte(msg)); err != nil {
		return "", err
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// waitFor polls cond until it holds or the budget runs out. Polling rather
// than sleeping a fixed time keeps the tests quick on an idle machine without
// making them fail on a loaded one.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// isTimeout separates "the peer never answered" from "the peer said no",
// which is the distinction most of these tests turn on.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestFaultStringAndParse(t *testing.T) {
	for _, f := range []Fault{Pass, Delay, Drop, Reset, Refuse} {
		s := f.String()
		if s == "" {
			t.Fatalf("fault %d has an empty name", uint8(f))
		}
		got, err := ParseFault(s)
		if err != nil {
			t.Fatalf("ParseFault(%q): %v", s, err)
		}
		if got != f {
			t.Fatalf("ParseFault(%q) = %v, want %v", s, got, f)
		}
	}
	if _, err := ParseFault("melt"); err == nil {
		t.Fatal("ParseFault accepted an unknown fault")
	}
	if s := Fault(200).String(); s == "" {
		t.Fatal("an out-of-range fault must still render something")
	}
}

// TestPassForwardsBothWays pushes a payload many times the copy buffer through
// the proxy, because a copy loop that mishandles short reads only shows up
// once the stream no longer fits in one buffer.
func TestPassForwardsBothWays(t *testing.T) {
	srv := echoServer(t)
	l := newTestLink(t, srv)

	c := dialLink(t, l)
	if err := c.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}

	// Deliberately not a multiple of the copy buffer: with an exact multiple
	// every read fills the buffer, and a copy loop that writes the whole
	// buffer instead of the bytes it actually read looks perfect. The odd
	// tail is what catches it.
	payload := make([]byte, 1<<20+12345)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("payload: %v", err)
	}

	werr := make(chan error, 1)
	go func() {
		_, err := c.Write(payload)
		werr <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if err := <-werr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(payload, got) {
		t.Fatal("the bytes that came back are not the bytes that went out")
	}

	// And nothing else came back. A copy loop that writes its whole buffer
	// rather than the part it filled corrupts only the tail, which the
	// comparison above would never see.
	if err := c.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	if n, err := c.Read(make([]byte, 1)); err == nil || n > 0 {
		t.Fatalf("the proxy produced %d bytes beyond the payload (err %v)", n, err)
	} else if !isTimeout(err) {
		t.Fatalf("unexpected error looking for trailing bytes: %v", err)
	}

	st := l.Stats()
	if st.Accepted != 1 {
		t.Fatalf("Accepted = %d, want 1", st.Accepted)
	}
	if st.BytesForward != int64(len(payload)) {
		t.Fatalf("BytesForward = %d, want exactly %d", st.BytesForward, len(payload))
	}
	if st.BytesBack != int64(len(payload)) {
		t.Fatalf("BytesBack = %d, want exactly %d", st.BytesBack, len(payload))
	}
	if st.BytesDropped != 0 {
		t.Fatalf("BytesDropped = %d on a healthy link, want 0", st.BytesDropped)
	}
}

// TestDropMidRequestHangs is the case the whole package exists for: the fault
// arrives while a connection is already open, and the client must be left
// hanging rather than told anything. A client that is told "connection reset"
// knows its request failed; a client that hangs does not, and that is what
// makes the operation indeterminate.
func TestDropMidRequestHangs(t *testing.T) {
	srv := echoServer(t)
	l := newTestLink(t, srv)
	c := dialLink(t, l)

	if got, err := roundTrip(c, "hello", 5*time.Second); err != nil || got != "hello" {
		t.Fatalf("healthy round trip: got %q err %v", got, err)
	}

	l.Set(Drop)
	if l.Fault() != Drop {
		t.Fatalf("Fault() = %v after Set(Drop)", l.Fault())
	}

	if _, err := c.Write([]byte("swallowed")); err != nil {
		t.Fatalf("write under Drop should still be accepted by the socket: %v", err)
	}
	if err := c.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	buf := make([]byte, 16)
	n, err := c.Read(buf)
	if err == nil {
		t.Fatalf("read returned %d bytes under Drop; the link is not a blackhole", n)
	}
	if !isTimeout(err) {
		t.Fatalf("read failed with %v, want a timeout: the peer must hang, not learn its request failed", err)
	}

	if !waitFor(2*time.Second, func() bool { return l.Stats().BytesDropped > 0 }) {
		t.Fatalf("BytesDropped = %d, want the swallowed request counted", l.Stats().BytesDropped)
	}
}

// TestDropHealsForNewConnections checks that a link recovers: after the
// blackhole lifts, a fresh connection works again.
func TestDropHealsForNewConnections(t *testing.T) {
	srv := echoServer(t)
	l := newTestLink(t, srv)

	l.Set(Drop)
	c1 := dialLink(t, l)
	if _, err := roundTrip(c1, "ping", 300*time.Millisecond); err == nil {
		t.Fatal("a round trip completed while the link was a blackhole")
	}
	c1.Close()

	l.Set(Pass)
	c2 := dialLink(t, l)
	if got, err := roundTrip(c2, "ping", 5*time.Second); err != nil || got != "ping" {
		t.Fatalf("after healing: got %q err %v", got, err)
	}
}

// TestResetBreaksOpenConnection asserts the opposite of Drop: the peer is told,
// promptly, that its connection is gone.
func TestResetBreaksOpenConnection(t *testing.T) {
	srv := echoServer(t)
	l := newTestLink(t, srv)
	c := dialLink(t, l)

	if got, err := roundTrip(c, "hello", 5*time.Second); err != nil || got != "hello" {
		t.Fatalf("healthy round trip: got %q err %v", got, err)
	}

	// A generous ceiling: if Reset only took effect on the next write, or
	// only for new connections, this read would sit here until the deadline.
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	start := time.Now()
	l.Set(Reset)
	buf := make([]byte, 16)
	_, err := c.Read(buf)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("read succeeded on a link that was reset")
	}
	if isTimeout(err) {
		t.Fatalf("open connection survived Reset for %v", elapsed)
	}
	t.Logf("open connection broken %v after Set(Reset)", elapsed)

	// Still in force: a connection opened now is accepted and then dropped.
	c2, err := net.DialTimeout("tcp", l.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial under Reset should still be accepted: %v", err)
	}
	defer c2.Close()
	if err := c2.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	if _, err := c2.Write([]byte("hello")); err != nil {
		t.Logf("write to a reset connection failed immediately: %v", err)
	}
	if _, err := c2.Read(buf); err == nil {
		t.Fatal("a connection opened under Reset served a reply")
	} else if isTimeout(err) {
		t.Fatal("a connection opened under Reset was left hanging, not closed")
	}

	if st := l.Stats(); st.ResetConns < 2 {
		t.Fatalf("ResetConns = %d, want at least 2", st.ResetConns)
	}

	l.Set(Pass)
	c3 := dialLink(t, l)
	if got, err := roundTrip(c3, "ping", 5*time.Second); err != nil || got != "ping" {
		t.Fatalf("after healing: got %q err %v", got, err)
	}
}

// TestRefuseRefusesDialsAndRecoversOnTheSamePort pins the behaviour the brief
// insists on: the port a client was told to use never changes.
func TestRefuseRefusesDialsAndRecoversOnTheSamePort(t *testing.T) {
	srv := echoServer(t)
	l := newTestLink(t, srv)
	addr := l.Addr()

	open := dialLink(t, l)
	if got, err := roundTrip(open, "hello", 5*time.Second); err != nil || got != "hello" {
		t.Fatalf("healthy round trip: got %q err %v", got, err)
	}

	l.Set(Refuse)

	// A refused dial is the point: the client learns its request was never
	// delivered. A dial that hangs instead would be indistinguishable from a
	// blackhole and would not prove anything.
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err == nil {
		c.Close()
		t.Fatal("dial succeeded while the link was refusing")
	}
	if isTimeout(err) {
		t.Fatalf("dial timed out instead of being refused: %v", err)
	}
	t.Logf("refused dial: %v", err)

	// Refuse also tears down connections that are already open, otherwise a
	// client holding a pooled connection never notices the fault.
	if err := open.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	if _, err := open.Read(make([]byte, 16)); err == nil {
		t.Fatal("an open connection survived Refuse")
	} else if isTimeout(err) {
		t.Fatal("an open connection was left hanging by Refuse")
	}
	if st := l.Stats(); st.Refused < 1 {
		t.Fatalf("Refused = %d, want at least 1", st.Refused)
	}

	l.Set(Pass)
	if l.Addr() != addr {
		t.Fatalf("Addr() moved from %s to %s across a Refuse; clients hold the old one", addr, l.Addr())
	}
	// Set is documented to return with the listener bound again, so this
	// needs no retry loop. If that ever stops being true on some platform,
	// this is the test that will say so.
	c2, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial after healing: %v", err)
	}
	defer c2.Close()
	if got, err := roundTrip(c2, "ping", 5*time.Second); err != nil || got != "ping" {
		t.Fatalf("after healing: got %q err %v", got, err)
	}
}

// TestDelayAddsRoundTripLatency asserts only the lower bound, which the test
// sets itself, plus a ceiling loose enough to survive a busy runner.
func TestDelayAddsRoundTripLatency(t *testing.T) {
	srv := echoServer(t)
	l := newTestLink(t, srv)
	c := dialLink(t, l)

	if got, err := roundTrip(c, "hello", 5*time.Second); err != nil || got != "hello" {
		t.Fatalf("healthy round trip: got %q err %v", got, err)
	}

	const hold = 150 * time.Millisecond
	l.SetDelay(hold)
	l.Set(Delay)
	if l.DelayFor() != hold {
		t.Fatalf("DelayFor() = %v, want %v", l.DelayFor(), hold)
	}

	start := time.Now()
	if got, err := roundTrip(c, "hello", 30*time.Second); err != nil || got != "hello" {
		t.Fatalf("delayed round trip: got %q err %v", got, err)
	}
	elapsed := time.Since(start)
	if elapsed < hold {
		t.Fatalf("round trip took %v, want at least the %v we asked for", elapsed, hold)
	}
	if elapsed > 30*time.Second {
		t.Fatalf("round trip took %v, far beyond the delay", elapsed)
	}
	t.Logf("round trip under a %v delay took %v (each direction is held, so about twice the delay)", hold, elapsed)

	l.Set(Pass)
	start = time.Now()
	if got, err := roundTrip(c, "hello", 30*time.Second); err != nil || got != "hello" {
		t.Fatalf("healed round trip: got %q err %v", got, err)
	}
	t.Logf("round trip after healing took %v", time.Since(start))
}

// TestSetUnderConcurrentTraffic exists for the race detector in CI. Every
// field a fault can change is read by live connection goroutines, so this
// drives Set from one goroutine while several others push bytes.
func TestSetUnderConcurrentTraffic(t *testing.T) {
	srv := echoServer(t)
	l := newTestLink(t, srv)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Paced deliberately. Under Reset and Refuse every attempt
				// fails at once, and an unpaced loop burns thousands of
				// ephemeral ports a second, which fails a CI runner for
				// reasons that have nothing to do with this package.
				time.Sleep(5 * time.Millisecond)

				c, err := net.DialTimeout("tcp", l.Addr(), time.Second)
				if err != nil {
					// Refused dials are expected here.
					continue
				}
				for j := 0; j < 5; j++ {
					if _, err := roundTrip(c, "tick", 200*time.Millisecond); err != nil {
						break
					}
				}
				c.Close()
			}
		}()
	}

	faults := []Fault{Pass, Delay, Drop, Pass, Reset, Pass, Refuse, Pass}
	l.SetDelay(5 * time.Millisecond)
	deadline := time.Now().Add(1500 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		l.Set(faults[i%len(faults)])
		l.SetDelay(time.Duration(i%7) * time.Millisecond)
		l.Stats()
		time.Sleep(10 * time.Millisecond)
	}
	l.Set(Pass)
	close(stop)
	wg.Wait()

	st := l.Stats()
	t.Logf("stats after the storm: %+v", st)
	if st.Accepted == 0 {
		t.Fatal("no connections were accepted at all")
	}
	c := dialLink(t, l)
	if got, err := roundTrip(c, "ping", 10*time.Second); err != nil || got != "ping" {
		t.Fatalf("link unusable after the storm: got %q err %v", got, err)
	}
}

// TestCloseIsIdempotentAndLeavesNoGoroutines proves Close waits for everything
// it started. A proxy that leaks a goroutine per connection would sink a long
// run, and the leak is invisible unless something counts.
//
// The count is taken at the instant Close returns, with no grace period,
// because "they exit shortly afterwards" is a different and much weaker
// promise than the one Close makes. That is only meaningful because the target
// used here runs no goroutine per connection, so everything above the baseline
// belongs to the link.
func TestCloseIsIdempotentAndLeavesNoGoroutines(t *testing.T) {
	srv := quietServer(t)

	// Settle first: the target's own goroutine must be in the baseline,
	// otherwise this measures it instead of the link's.
	runtime.GC()
	base := runtime.NumGoroutine()

	l, err := NewLink("leaky", srv)
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	var conns []net.Conn
	for i := 0; i < 30; i++ {
		c, err := net.DialTimeout("tcp", l.Addr(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conns = append(conns, c)
		if err := c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("deadline: %v", err)
		}
		if _, err := c.Write([]byte("hello")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Half the connections are left mid-blackhole and half mid-delay, with a
	// delay far longer than the test: Close has to break its goroutines out
	// of both, not wait for them to finish naturally.
	l.SetDelay(30 * time.Second)
	l.Set(Delay)
	for _, c := range conns[:len(conns)/2] {
		c.Write([]byte("stuck"))
	}
	time.Sleep(50 * time.Millisecond)
	l.Set(Drop)
	for _, c := range conns[len(conns)/2:] {
		c.Write([]byte("swallowed"))
	}
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closeTook := time.Since(start)
	immediate := runtime.NumGoroutine()
	t.Logf("Close returned in %v with %d goroutines against a baseline of %d", closeTook, immediate, base)

	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for _, c := range conns {
		c.Close()
	}

	if immediate > base {
		// Distinguish the two failures, because they need different fixes:
		// a missing wait, or a goroutine that never comes back at all.
		if waitFor(5*time.Second, func() bool { return runtime.NumGoroutine() <= base }) {
			t.Fatalf("Close returned with %d goroutines still running against a baseline of %d; they left afterwards, so Close is not waiting for them",
				immediate, base)
		}
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutines %d > baseline %d and never left\n%s", runtime.NumGoroutine(), base, buf[:n])
	}

	// A closed link accepts no more connections.
	if c, err := net.DialTimeout("tcp", l.Addr(), time.Second); err == nil {
		c.Close()
		t.Fatal("a closed link still accepts connections")
	}
}

func TestSetOnClosedLinkIsHarmless(t *testing.T) {
	srv := echoServer(t)
	l, err := NewLink("gone", srv)
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A nemesis can still be holding this link when the run tears down.
	l.Set(Refuse)
	l.Set(Pass)
	l.SetDelay(time.Second)
	_ = l.Stats()
	if l.Name() != "gone" || l.Target() != srv {
		t.Fatalf("Name/Target changed: %q %q", l.Name(), l.Target())
	}
}

func TestNewLinkRejectsAnEmptyTarget(t *testing.T) {
	if l, err := NewLink("x", ""); err == nil {
		l.Close()
		t.Fatal("NewLink accepted an empty target")
	}
}
