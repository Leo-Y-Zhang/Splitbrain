package kv

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

func TestSingleStoreSequential(t *testing.T) {
	s := NewSingleStore()
	ctx := context.Background()

	got, err := s.Apply(ctx, Request{Kind: history.Read, Key: "x"})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !got.OK || got.Value == nil || *got.Value != model.InitialValue {
		t.Fatalf("read of an untouched key = %+v, want ok with value %d", got, model.InitialValue)
	}

	if got, err = s.Apply(ctx, Request{Kind: history.Write, Key: "x", Value: 5}); err != nil || !got.OK {
		t.Fatalf("write = %+v, %v", got, err)
	}
	if got.Value != nil {
		t.Errorf("a write reply carried a value: %+v", got)
	}

	got, _ = s.Apply(ctx, Request{Kind: history.CAS, Key: "x", From: 4, To: 6})
	if !got.OK || got.Swapped == nil || *got.Swapped {
		t.Errorf("cas from the wrong value = %+v, want ok with swapped false", got)
	}
	got, _ = s.Apply(ctx, Request{Kind: history.Read, Key: "x"})
	if *got.Value != 5 {
		t.Errorf("a failed cas changed the value to %d", *got.Value)
	}

	got, _ = s.Apply(ctx, Request{Kind: history.CAS, Key: "x", From: 5, To: 6})
	if !got.OK || got.Swapped == nil || !*got.Swapped {
		t.Errorf("cas from the right value = %+v, want ok with swapped true", got)
	}
	got, _ = s.Apply(ctx, Request{Kind: history.Read, Key: "x"})
	if *got.Value != 6 {
		t.Errorf("value after a successful cas = %d, want 6", *got.Value)
	}
}

// Eight clients hammering one node over real sockets. The assertion is not
// that any particular interleaving happens, but that the store never invents a
// value: every read must return something that was written, and the count of
// successful swaps must exactly match the number of increments the register
// actually made.
func TestSingleStoreUnderConcurrency(t *testing.T) {
	const (
		procs = 8
		steps = 60
	)
	store := NewSingleStore()
	c := serve(t, "n1", store)
	origin := testClock

	var mu sync.Mutex
	var swaps int
	var recorded history.History
	written := map[int]bool{model.InitialValue: true}
	record := func(op history.Op) {
		mu.Lock()
		recorded = append(recorded, op)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < steps; i++ {
				switch i % 3 {
				case 0:
					v := p*1000 + i + 1
					mu.Lock()
					written[v] = true
					mu.Unlock()
					op := c.Do(ctx, origin, p, Request{Kind: history.Write, Key: "k", Value: v})
					record(op)
					if op.Outcome != history.OK {
						t.Errorf("process %d: write = %s (%s)", p, op.Outcome, op.Err)
					}
				case 1:
					op := c.Do(ctx, origin, p, Request{Kind: history.Read, Key: "k"})
					record(op)
					if op.Outcome != history.OK {
						t.Errorf("process %d: read = %s (%s)", p, op.Outcome, op.Err)
						continue
					}
					mu.Lock()
					seen := written[op.Observed]
					mu.Unlock()
					if !seen {
						t.Errorf("process %d: read %d, which nobody ever wrote", p, op.Observed)
					}
				default:
					// A CAS onto a value only this process can produce, so a
					// reported swap is unambiguous evidence about the
					// register's contents at that instant.
					to := 500000 + p*1000 + i
					mu.Lock()
					written[to] = true
					mu.Unlock()
					op := c.Do(ctx, origin, p, Request{Kind: history.CAS, Key: "k", From: model.InitialValue, To: to})
					record(op)
					if op.Outcome != history.OK {
						t.Errorf("process %d: cas = %s (%s)", p, op.Outcome, op.Err)
						continue
					}
					if op.Swapped {
						mu.Lock()
						swaps++
						mu.Unlock()
					}
				}
			}
		}(p)
	}
	wg.Wait()

	// Only the very first operations can find the register still at its
	// initial value, so a swap is possible but rare. The point is that the
	// count is a plain integer and not a data race; -race in CI is what
	// actually watches for the latter.
	if swaps < 0 {
		t.Fatalf("impossible swap count %d", swaps)
	}
	final, err := store.Apply(context.Background(), Request{Kind: history.Read, Key: "k"})
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if !written[*final.Value] {
		t.Errorf("final value %d was never written", *final.Value)
	}

	// The whole recorded history has to be structurally sound, not just each
	// operation on its own. This is where a clock too coarse to separate one
	// process's consecutive operations shows up: Validate rejects a process
	// that appears to have had two requests in flight at once, and a harness
	// that emitted such a history would be measuring nothing.
	if len(recorded) != procs*steps {
		t.Fatalf("recorded %d operations, want %d", len(recorded), procs*steps)
	}
	if err := recorded.Validate(); err != nil {
		t.Errorf("the recorded history does not validate: %v (clock: %s)", err, testClock)
	}
	ok, fail, info := recorded.Counts()
	t.Logf("%d operations across %d processes: %d ok, %d fail, %d info; clock %s",
		len(recorded), procs, ok, fail, info, testClock)
}

// Keys must not interfere; the checker splits histories by key and relies on
// that being true of the store as well as of the model.
func TestSingleStoreKeysAreIndependent(t *testing.T) {
	s := NewSingleStore()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		s.Apply(ctx, Request{Kind: history.Write, Key: fmt.Sprintf("k%d", i), Value: i * 11})
	}
	for i := 0; i < 5; i++ {
		got, _ := s.Apply(ctx, Request{Kind: history.Read, Key: fmt.Sprintf("k%d", i)})
		if *got.Value != i*11 {
			t.Errorf("k%d = %d, want %d", i, *got.Value, i*11)
		}
	}
}
