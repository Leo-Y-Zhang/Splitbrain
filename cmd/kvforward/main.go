// Command kvforward serves one node of a three-node store with a fixed leader.
//
// The leader holds the data; a follower holds nothing and forwards every
// operation to it. Everything therefore serialises at one mutex in one
// process, so the arrangement is linearizable, and it gives up availability
// the moment a follower cannot reach the leader: clients see errors and
// timeouts rather than wrong answers. That is the CP corner.
//
// Run it with -leader empty to be the leader, or with -leader pointing at one.
// The harness usually supplies neither, because the leader's port is not known
// until it has started, and sends POST /configure instead.
//
// It prints "listening <addr>" on stdout once, and nothing else ever goes
// there; logs go to stderr. It stops on SIGINT, on SIGTERM and when stdin
// reaches EOF.
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/kv"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "address to listen on; port 0 lets the kernel choose")
	id := flag.String("id", "kvforward", "node identifier, reported by /health and in every log line")
	leader := flag.String("leader", "", "address of the leader to forward to; empty means this node is the leader")
	timeout := flag.Duration("timeout", 250*time.Millisecond, "bound on one forwarded request, dial to last byte")
	promote := flag.Bool("promote", false, "let a follower that has lost contact appoint itself leader and serve from a cache; see the kv.ForwardStore documentation for why that is a deliberate split-brain fixture")
	promoteAfter := flag.Int("promote-after", 3, "consecutive failed forwards before a follower promotes itself, when -promote is set")
	flag.Parse()

	logger := kv.NewLogger(os.Stderr)
	store := kv.NewForwardStore(*leader, *timeout, logger)
	// The flags and POST /configure reach the same fields through the same
	// code path, so there is only one place for the two to disagree.
	if err := store.Configure(kv.Config{Promote: promote, PromoteAfter: promoteAfter}); err != nil {
		logger.Error("configuring from flags", "node", *id, "error", err)
		os.Exit(1)
	}
	if err := kv.Run(context.Background(), *addr, *id, store, logger); err != nil {
		logger.Error("stopped with an error", "node", *id, "error", err)
		os.Exit(1)
	}
}
