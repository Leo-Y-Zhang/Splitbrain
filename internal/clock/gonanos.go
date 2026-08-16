package clock

import "time"

// processStart anchors Go's monotonic clock. time.Time carries a monotonic
// reading, and subtracting two of them uses it, so this is a monotonic base
// even though the wall clock may jump.
var processStart = time.Now()

// goNanos is the portable reading: nanoseconds since the process started,
// from Go's own monotonic clock. On Windows it is the fallback for when the
// performance counter is unavailable, which should not happen but is not worth
// crashing over.
func goNanos() int64 { return int64(time.Since(processStart)) }
