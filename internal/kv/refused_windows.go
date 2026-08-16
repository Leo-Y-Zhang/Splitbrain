//go:build windows

package kv

import "syscall"

// connRefused is WSAECONNREFUSED, the Winsock error a refused TCP connect
// returns on Windows. It is spelled as a literal because Go's syscall package
// stops short of defining this particular WSAE constant, and because
// syscall.ECONNREFUSED on Windows is a synthetic value that never appears on a
// socket. The number is fixed by Winsock and documented by Microsoft.
const connRefused = syscall.Errno(10061)
