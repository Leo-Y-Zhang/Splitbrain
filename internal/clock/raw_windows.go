//go:build windows

package clock

import (
	"sync"
	"syscall"
	"unsafe"
)

// Windows gets the performance counter rather than Go's monotonic clock.
//
// Go's runtime clock here advances in steps of roughly half a millisecond,
// measured on the machine this was written on: two million back-to-back
// readings, all but eight identical, smallest non-zero step 523 microseconds.
// A loopback request takes less than that, so most operations would record as
// instantaneous and the real-time order in the history would be invention.
//
// QueryPerformanceCounter is typically a ten-megahertz counter, which is a
// hundred nanoseconds - four thousand times finer, and enough that the
// widening in Completion costs nothing. It is the same source Windows itself
// recommends for interval timing.
const rawSource = "QueryPerformanceCounter"

var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")
	procQPC  = kernel32.NewProc("QueryPerformanceCounter")
	procQPF  = kernel32.NewProc("QueryPerformanceFrequency")

	freqOnce sync.Once
	// ticksPerSecond is fixed at boot on every Windows version that matters,
	// so it is read once. Zero means the call failed, and rawNanos falls back.
	ticksPerSecond int64
)

func frequency() int64 {
	freqOnce.Do(func() {
		var f int64
		r, _, _ := procQPF.Call(uintptr(unsafe.Pointer(&f)))
		if r != 0 && f > 0 {
			ticksPerSecond = f
		}
	})
	return ticksPerSecond
}

// rawNanos returns a monotonic reading in nanoseconds. The absolute value is
// meaningless; only differences are used.
func rawNanos() int64 {
	f := frequency()
	if f == 0 {
		return goNanos()
	}
	var ticks int64
	r, _, _ := procQPC.Call(uintptr(unsafe.Pointer(&ticks)))
	if r == 0 {
		return goNanos()
	}
	// Split the conversion so a counter that has been running for months does
	// not overflow on the multiply. At ten megahertz the plain form overflows
	// after about twenty-nine years of uptime, which is not a risk worth
	// taking for one extra division.
	return (ticks/f)*1_000_000_000 + (ticks%f)*1_000_000_000/f
}
