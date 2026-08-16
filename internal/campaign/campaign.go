// Package campaign runs one complete experiment: start a cluster, put fault
// proxies in front of every hop, drive load through a seeded partition
// schedule, and check the resulting history.
//
// It exists so the command-line tool and the end-to-end demonstration cannot
// drift apart. If the demonstration proved its claim through a different code
// path from the one users run, the claim would be about the demonstration.
package campaign

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/checker"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/cluster"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/harness"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// A Spec is one experiment.
type Spec struct {
	Target      cluster.Target
	Nodes       int
	BinDir      string
	NodeTimeout time.Duration
	Harness     harness.Config
	Checker     checker.Options
	Model       model.Model
	// NodeStderr receives the nodes' logs. Nil discards them.
	NodeStderr io.Writer
}

// An Outcome is what one experiment produced.
type Outcome struct {
	Run   *harness.Result
	Check checker.Result
	Addrs []string
	Links []string
}

// Once runs a single experiment end to end.
func Once(ctx context.Context, spec Spec) (Outcome, error) {
	if !spec.Target.Valid() {
		return Outcome{}, fmt.Errorf("campaign: unknown target %q", spec.Target)
	}
	m := spec.Model
	if m == nil {
		m = model.CASRegister{}
	}
	if spec.NodeTimeout <= 0 {
		spec.NodeTimeout = 250 * time.Millisecond
	}

	c, err := cluster.Start(ctx, cluster.Options{
		Target:  spec.Target,
		Nodes:   spec.Nodes,
		BinDir:  spec.BinDir,
		Timeout: spec.NodeTimeout,
		Stderr:  spec.NodeStderr,
	})
	if err != nil {
		return Outcome{}, err
	}
	defer c.Close()

	topo, err := harness.BuildTopology(c.Addrs(), spec.Target.Replicated())
	if err != nil {
		return Outcome{}, err
	}
	defer topo.Close()

	// The nodes reach each other through proxies too. Without this a
	// partition cuts the clients off while the replicas carry on gossiping
	// behind it, they never diverge, and the run proves nothing.
	if err := c.Configure(ctx, c.DefaultConfig(topo.PeerAddr)); err != nil {
		return Outcome{}, err
	}

	res, err := harness.Run(ctx, topo, spec.Harness)
	if err != nil {
		return Outcome{}, err
	}

	verdict, err := checker.Check(res.History, m, spec.Checker)
	if err != nil {
		return Outcome{}, err
	}

	return Outcome{Run: res, Check: verdict, Addrs: c.Addrs(), Links: topo.LinkNames()}, nil
}

// DroppedBytes totals what the fault layer swallowed. A run whose schedule
// promised partitions but dropped nothing did not partition anything, and its
// clean verdict means nothing - so this is worth asserting on, not just
// printing.
func (o Outcome) DroppedBytes() int64 {
	var n int64
	for _, s := range o.Run.LinkStats {
		n += s.BytesDropped
	}
	return n
}

// ForwardedBytes totals what actually reached the nodes.
func (o Outcome) ForwardedBytes() int64 {
	var n int64
	for _, s := range o.Run.LinkStats {
		n += s.BytesForward
	}
	return n
}
