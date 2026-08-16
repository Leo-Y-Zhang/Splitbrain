// Command kvquorum serves one node of a three-node store that is deliberately
// not linearizable.
//
// Each node keeps its own map. Writes are applied locally and fanned out to
// the peers in the background, best-effort; reads are answered locally and
// never consult anybody; conflicts resolve last-writer-wins on a wall-clock
// timestamp tie-broken by node id. It stays available under a partition and it
// returns stale values, which is the AP corner and the whole reason the
// checker exists.
//
// It is a fixture, not a strawman. It models the design people write before
// they have met consensus, and every piece of it is individually reasonable;
// see the documentation on kv.QuorumStore.
//
// The harness usually starts it with no -peers, because the other nodes' ports
// are not known until they have started, and sends POST /configure instead.
//
// It prints "listening <addr>" on stdout once, and nothing else ever goes
// there; logs go to stderr. It stops on SIGINT, on SIGTERM and when stdin
// reaches EOF.
package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/kv"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "address to listen on; port 0 lets the kernel choose")
	id := flag.String("id", "kvquorum", "node identifier, used to break last-writer-wins ties and to tag log lines")
	peers := flag.String("peers", "", "comma-separated addresses of the other nodes to replicate to")
	timeout := flag.Duration("timeout", 250*time.Millisecond, "bound on one outbound replication, dial to last byte")
	synchronous := flag.Bool("sync", false, "wait for every peer before replying to a write, and report success anyway; see the kv.QuorumStore documentation for why that is a deliberately different way to be wrong")
	flag.Parse()

	logger := kv.NewLogger(os.Stderr)
	store := kv.NewQuorumStore(*id, splitPeers(*peers), *timeout, logger)
	// The flag and POST /configure reach the same fields through the same
	// code path, so there is only one place for the two to disagree.
	if err := store.Configure(kv.Config{Sync: synchronous}); err != nil {
		logger.Error("configuring from flags", "node", *id, "error", err)
		os.Exit(1)
	}
	if err := kv.Run(context.Background(), *addr, *id, store, logger); err != nil {
		logger.Error("stopped with an error", "node", *id, "error", err)
		os.Exit(1)
	}
}

// splitPeers turns the comma-separated flag into addresses, discarding empty
// entries so that -peers "" and a trailing comma both mean "no peers" rather
// than "one peer with no address".
func splitPeers(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
