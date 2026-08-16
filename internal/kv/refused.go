package kv

import (
	"errors"
	"net"
	"syscall"
)

// isConnRefused reports whether err is a TCP connection that was refused while
// being established.
//
// This is the only transport failure a client may record as history.Fail, so
// it is worth being fussy about. Two conditions have to hold together: the
// failure happened during the dial, and the operating system's reason was
// "connection refused". A refusal means the kernel on the far side answered
// with RST because nothing was listening, so no byte of the request was ever
// handed to a server process and the operation certainly did not happen.
//
// It is deliberately conservative. Every case it declines becomes
// history.Info, which is the weaker and always-sound classification; a false
// positive here would delete a real operation from the history and could turn
// a genuine violation into a pass.
func isConnRefused(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		return false
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	// Both constants are checked on every platform. On Unix a refused dial
	// arrives as ECONNREFUSED (111 on Linux). On Windows it arrives as raw
	// Winsock error 10061, WSAECONNREFUSED; Go's syscall package does define
	// ECONNREFUSED there, but as a synthetic APPLICATION_ERROR value
	// (536870934) that no socket ever returns, so errors.Is against it is
	// false for a genuine refusal. connRefused holds whichever value the
	// build's platform actually produces.
	return errno == syscall.ECONNREFUSED || errno == connRefused
}
