// Command kvsingle serves one key-value map from one process behind one mutex.
//
// It is linearizable by construction and it is the control case for the whole
// tool: a violation reported against a run of kvsingle is evidence of a bug in
// the harness or the checker rather than in the store. It is also the least
// available of the three, in the plainest possible way - there is one node, so
// cutting it off produces no answers rather than wrong ones.
//
// It prints "listening <addr>" on stdout once, and nothing else ever goes
// there; logs go to stderr. It stops on SIGINT, on SIGTERM and when stdin
// reaches EOF.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/kv"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "address to listen on; port 0 lets the kernel choose")
	id := flag.String("id", "kvsingle", "node identifier, reported by /health and in every log line")
	flag.Duration("timeout", 0, "accepted so every node takes the same arguments, and ignored: a single node never talks to a peer")
	flag.Parse()

	logger := kv.NewLogger(os.Stderr)
	if err := kv.Run(context.Background(), *addr, *id, kv.NewSingleStore(), logger); err != nil {
		logger.Error("stopped with an error", "node", *id, "error", err)
		os.Exit(1)
	}
}
