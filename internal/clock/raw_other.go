//go:build !windows

package clock

// Everywhere except Windows, Go's own monotonic clock is already as fine as
// the platform offers - nanosecond steps from clock_gettime on Linux - so
// there is nothing to improve on and no reason to reach for a system call.
const rawSource = "Go monotonic clock"

func rawNanos() int64 { return goNanos() }
