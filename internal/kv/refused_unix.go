//go:build !windows

package kv

import "syscall"

// connRefused is the errno a refused TCP connect returns. Everywhere except
// Windows that is ECONNREFUSED, so the platform-specific check collapses into
// the portable one and isConnRefused compares the same value twice.
const connRefused = syscall.ECONNREFUSED
