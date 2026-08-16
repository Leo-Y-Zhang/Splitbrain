// Package faultnet is the network fault layer of the harness: a real userspace
// TCP proxy that can be told, at any instant, to blackhole, delay, reset or
// refuse a link.
//
// It listens on a real socket, dials a real backend and shovels real bytes.
// Because it works on the raw byte stream it neither knows nor cares what
// protocol is running over it, so the same code partitions HTTP, RESP and gRPC
// alike. That is what makes the partitions in the test suite real rather than
// simulated: the system under test is a set of separate operating system
// processes talking over loopback TCP, and the break happens underneath them
// exactly as a broken network would.
//
// What it is not: it is not the kernel. A proxy can only interfere with bytes
// once a connection has been accepted, so a client dialling a blackholed link
// still completes its TCP handshake, where a real dropped SYN would leave it
// waiting. Every deviation of that sort is written down on the fault it
// belongs to.
package faultnet

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// A Fault is the condition in force on one link.
type Fault uint8

const (
	// Pass lets bytes flow normally in both directions.
	Pass Fault = iota

	// Delay lets bytes flow, holding each chunk back for the duration set
	// with Link.SetDelay and reported by Link.DelayFor. Both directions are
	// held, so a request/response round trip gains about twice the delay,
	// which is what a slow link does to a real client.
	//
	// A chunk already being held is not let go early when the link heals, so
	// a link recovers from a long delay one delay after the heal rather than
	// instantly. Only Close cuts a hold short.
	Delay

	// Drop swallows bytes in both directions while leaving both sockets
	// open, so the peer sees a hang rather than an error.
	//
	// This is a blackhole partition and it is the interesting one: a client
	// that is told "connection reset" knows its request failed, whereas a
	// client that simply waits cannot tell whether the server applied the
	// write or never saw it. Those indeterminate operations are the ones
	// that break naive stores, so the fault that produces them earns its
	// keep.
	//
	// Trap: bytes swallowed here are gone for good, where a real blackhole
	// would have TCP retransmit them after the heal. A connection that lived
	// through a Drop therefore has a hole in its stream and should be
	// treated as spoilt; in practice the client times out and redials, which
	// is the behaviour worth testing anyway.
	//
	// A connection accepted while the link is blackholed still opens a
	// socket to the target, which a dropped SYN would not have done, so the
	// target sees an idle connection it will never read a byte from. Keeping
	// the pair symmetric is what lets bytes flow again the instant the link
	// heals.
	Drop

	// Reset closes open connections immediately, and closes new ones as soon
	// as they are accepted, so the peer sees a broken connection rather than
	// a hang.
	Reset

	// Refuse closes the listener, so a dial is refused by the operating
	// system and the client can prove its request was never delivered - the
	// one transport failure a history may record as a definite failure
	// rather than an indeterminate one.
	//
	// It also tears down connections that are already open. A client holding
	// a pooled keep-alive connection would otherwise sail straight through a
	// refusing link and never learn anything, which would make the fault
	// invisible to exactly the clients most worth testing.
	Refuse
)

// String renders a Fault for schedules, reports and logs.
func (f Fault) String() string {
	switch f {
	case Pass:
		return "pass"
	case Delay:
		return "delay"
	case Drop:
		return "drop"
	case Reset:
		return "reset"
	case Refuse:
		return "refuse"
	default:
		return fmt.Sprintf("fault(%d)", uint8(f))
	}
}

// ParseFault is the inverse of Fault.String.
func ParseFault(s string) (Fault, error) {
	switch s {
	case "pass":
		return Pass, nil
	case "delay":
		return Delay, nil
	case "drop":
		return Drop, nil
	case "reset":
		return Reset, nil
	case "refuse":
		return Refuse, nil
	default:
		return 0, fmt.Errorf("unknown fault %q (have: pass, delay, drop, reset, refuse)", s)
	}
}

// LinkStats is a snapshot of one link's counters. Everything here is
// cumulative over the life of the link.
type LinkStats struct {
	// Accepted counts every connection the listener accepted, including
	// those immediately turned away under Reset or Refuse.
	Accepted int64

	// Refused counts connections turned away because the link was refusing:
	// those torn down when Refuse was applied, plus any accepted in the
	// moment between the fault taking hold and the listener closing.
	//
	// Dials the operating system refuses on our behalf are the common case
	// and are deliberately not counted here, because with no listener there
	// is nothing left in this process to see them.
	Refused int64

	// ResetConns counts connections broken by Reset: those already open when
	// it was applied, and those accepted while it was in force.
	ResetConns int64

	// BytesForward counts bytes written to the target.
	BytesForward int64

	// BytesBack counts bytes written back to the client.
	BytesBack int64

	// BytesDropped counts bytes read and thrown away under Drop, in either
	// direction.
	BytesDropped int64
}

// copyBuf is the per-direction copy buffer. Large enough that a bulk transfer
// is not syscall-bound, small enough that a Drop or a heal is noticed after at
// most this much data rather than at the end of a huge write.
const copyBuf = 32 * 1024

// dialTimeout bounds how long a proxied connection waits for the target. The
// targets here are local processes, so anything slower than this is a hang,
// not latency.
const dialTimeout = 5 * time.Second

// rebindWindow bounds how long Set waits to get the listener back on its
// original port when a link leaves Refuse. Measured on Windows over 200
// close/re-listen cycles the first attempt always succeeded, and Linux sets
// SO_REUSEADDR on listeners, so this budget exists only for the case where
// something else in the machine grabs the port in the gap.
const rebindWindow = time.Second

// A Link is one proxied TCP hop: clients dial Link.Addr(), bytes reach Target.
//
// Every method is safe to call from any goroutine, which is not a nicety - the
// nemesis changes faults on its own clock while connection goroutines are
// blocked in the middle of a copy.
type Link struct {
	name   string
	target string
	addr   string

	// fault and delay are read on every chunk by every live connection and
	// written by whichever goroutine is driving the schedule, so they are
	// atomic rather than mutex-guarded: a copy loop must never block waiting
	// to find out what it is meant to be doing.
	fault atomic.Uint32
	delay atomic.Int64

	accepted     atomic.Int64
	refused      atomic.Int64
	resetConns   atomic.Int64
	bytesForward atomic.Int64
	bytesBack    atomic.Int64
	bytesDropped atomic.Int64

	// ctx is a lifetime, not a request: it is cancelled by Close and is the
	// only thing that can interrupt a Delay hold or an in-flight dial.
	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex
	ln net.Listener
	// conns maps every socket this link owns to whether it is the client
	// side, which is what lets a teardown count connections rather than
	// sockets.
	conns  map[net.Conn]bool
	closed bool
	wg     sync.WaitGroup
}

// NewLink starts a proxy from a fresh loopback port to target. Name identifies
// the link in schedules, reports and logs.
func NewLink(name, target string) (*Link, error) {
	if target == "" {
		return nil, errors.New("faultnet: a link needs a target address")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("faultnet: link %s: %w", name, err)
	}
	l := &Link{
		name:   name,
		target: target,
		addr:   ln.Addr().String(),
		ln:     ln,
		conns:  make(map[net.Conn]bool),
	}
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.wg.Add(1)
	go l.accept(ln)
	return l, nil
}

// Addr is the loopback address clients should dial. It is fixed for the life
// of the link and survives Refuse, so a client may hold on to it.
func (l *Link) Addr() string { return l.addr }

// Name is the link's name, as used in a Schedule.
func (l *Link) Name() string { return l.name }

// Target is the backend address this link proxies to.
func (l *Link) Target() string { return l.target }

// Fault reports the fault in force.
func (l *Link) Fault() Fault { return Fault(l.fault.Load()) }

// DelayFor reports the hold applied to each chunk under Delay.
func (l *Link) DelayFor() time.Duration { return time.Duration(l.delay.Load()) }

// SetDelay sets the hold applied to each chunk under Delay. It takes effect on
// the next chunk in flight, so it can be changed while a link is delaying.
func (l *Link) SetDelay(d time.Duration) { l.delay.Store(int64(d)) }

// Set applies a fault to the link, including to connections that are already
// open - switching to Drop while a request is in flight is the whole point of
// the package.
//
// It is safe to call from any goroutine and does not block on connection
// goroutines. Leaving Refuse it does block, briefly, while the listener is
// bound again on its original port; it returns only once the link is listening
// again, so a caller may dial immediately afterwards. In the pathological case
// where something else in the machine has taken the port and it cannot be
// reclaimed within a second, the failure is logged and the link stays dark;
// the next Set tries again.
//
// A value outside the known faults is stored as given and behaves as Pass.
func (l *Link) Set(f Fault) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.fault.Store(uint32(f))

	switch f {
	case Refuse:
		l.refused.Add(l.tearDownLocked())
		if l.ln != nil {
			l.ln.Close()
			l.ln = nil
		}
	case Reset:
		l.resetConns.Add(l.tearDownLocked())
		l.listenLocked()
	default:
		l.listenLocked()
	}
}

// Stats snapshots the link's counters. The fields are read one at a time, so a
// snapshot taken during heavy traffic is a close reading rather than an
// instant.
func (l *Link) Stats() LinkStats {
	return LinkStats{
		Accepted:     l.accepted.Load(),
		Refused:      l.refused.Load(),
		ResetConns:   l.resetConns.Load(),
		BytesForward: l.bytesForward.Load(),
		BytesBack:    l.bytesBack.Load(),
		BytesDropped: l.bytesDropped.Load(),
	}
}

// Close shuts the link down: the listener and every connection are closed, and
// Close returns only once every goroutine the link started has exited. It is
// idempotent, and a link that has been closed ignores further faults.
func (l *Link) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.cancel()
	var err error
	if l.ln != nil {
		err = l.ln.Close()
		l.ln = nil
	}
	l.tearDownLocked()
	l.mu.Unlock()

	// Outside the lock: the goroutines being waited for need it to deregister.
	l.wg.Wait()
	return err
}

// listenLocked brings the listener back if it is down. Callers hold l.mu.
func (l *Link) listenLocked() {
	if l.ln != nil || l.closed {
		return
	}
	deadline := time.Now().Add(rebindWindow)
	for {
		ln, err := net.Listen("tcp", l.addr)
		if err == nil {
			l.ln = ln
			l.wg.Add(1)
			go l.accept(ln)
			return
		}
		if time.Now().After(deadline) {
			log.Printf("faultnet: link %s could not reclaim %s: %v", l.name, l.addr, err)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// tearDownLocked closes every socket the link owns and reports how many of
// them were client connections. Callers hold l.mu.
//
// The sockets are dropped from the map here so that two teardowns in a row do
// not count the same connection twice; the owning goroutine's deregistration
// afterwards is a no-op.
func (l *Link) tearDownLocked() int64 {
	var clients int64
	for c, isClient := range l.conns {
		if isClient {
			clients++
		}
		hardClose(c)
		delete(l.conns, c)
	}
	return clients
}

// accept serves one listener until it is closed. Each listener gets its own
// goroutine, so the one left over from a Refuse cannot steal connections from
// the listener that replaced it.
func (l *Link) accept(ln net.Listener) {
	defer l.wg.Done()
	for {
		c, err := ln.Accept()
		if err != nil {
			// On loopback the only error worth distinguishing is closure,
			// and there is nowhere useful to report anything else to, so
			// this listener's life ends either way.
			return
		}
		l.accepted.Add(1)
		if !l.register(c, true) {
			hardClose(c)
			return
		}
		go l.handle(c)
	}
}

// register adds a socket to the link and takes a reference on the wait group.
// It reports false if the link is closing, in which case the caller owns the
// socket and must close it.
func (l *Link) register(c net.Conn, isClient bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.conns[c] = isClient
	// Added under the same lock that sets closed, so this cannot race with
	// the Wait in Close.
	l.wg.Add(1)
	return true
}

// release closes a socket, forgets it and drops its wait-group reference.
func (l *Link) release(c net.Conn) {
	l.mu.Lock()
	delete(l.conns, c)
	l.mu.Unlock()
	c.Close()
	l.wg.Done()
}

// handle proxies one accepted connection for its lifetime.
func (l *Link) handle(client net.Conn) {
	defer l.release(client)

	switch l.Fault() {
	case Refuse:
		// Slipped in between the fault taking hold and the listener
		// closing. Turning it away is what a closed listener would have
		// done, a moment earlier.
		l.refused.Add(1)
		hardClose(client)
		return
	case Reset:
		l.resetConns.Add(1)
		hardClose(client)
		return
	}

	ctx, cancel := context.WithTimeout(l.ctx, dialTimeout)
	defer cancel()
	var d net.Dialer
	target, err := d.DialContext(ctx, "tcp", l.target)
	if err != nil {
		// Nothing to proxy to. Hanging up is honest: the client sees the
		// connection close, which is indeterminate, and that is exactly
		// what it was.
		return
	}
	if !l.register(target, false) {
		hardClose(target)
		return
	}
	defer l.release(target)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		l.pump(target, client, &l.bytesBack)
	}()
	l.pump(client, target, &l.bytesForward)
	wg.Wait()
}

// pump copies one direction, consulting the fault on every chunk.
//
// The fault is read after the read and before the write, which is what lets a
// fault applied mid-request take effect on bytes that are already in this
// process. There is no polling: a connection with nothing to say blocks in
// Read and costs nothing, and the faults that must act on an idle connection
// (Reset, Refuse, Close) close the socket, which wakes it.
func (l *Link) pump(src, dst net.Conn, forwarded *atomic.Int64) {
	// Propagating the end of the stream rather than closing the whole
	// connection means a client that has finished sending still receives the
	// rest of the reply.
	defer closeWrite(dst)

	buf := make([]byte, copyBuf)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			switch l.Fault() {
			case Drop:
				l.bytesDropped.Add(int64(n))
			case Reset, Refuse:
				// The link is being torn down under us; these bytes are
				// as lost as the ones a reset would have discarded.
				l.bytesDropped.Add(int64(n))
				return
			case Delay:
				if !l.hold(l.DelayFor()) {
					return
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
				forwarded.Add(int64(n))
			default:
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
				forwarded.Add(int64(n))
			}
		}
		if err != nil {
			return
		}
	}
}

// hold waits out a delay, reporting false if the link closed first so that
// Close is never held up by a long delay.
func (l *Link) hold(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-l.ctx.Done():
		return false
	}
}

// hardClose closes a socket so the peer sees a reset rather than a clean
// end-of-stream. The difference matters to the system under test: an EOF at
// the wrong moment looks like an orderly shutdown, a reset looks like the
// network breaking, and only the second is what this package is simulating.
func hardClose(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		// Errors here mean the socket is already gone, which is fine.
		_ = tc.SetLinger(0)
	}
	c.Close()
}

// closeWrite half-closes a socket, passing on the end of the stream.
func closeWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
}
