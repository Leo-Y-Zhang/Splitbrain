package kv

import (
	"context"
	"sync"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// SingleStore is one map behind one mutex in one process.
//
// It is linearizable by construction: every operation takes effect at the
// instant it holds the lock, and that instant lies inside the interval the
// client measured. That makes it the control case for the whole tool. A
// violation reported against a run of SingleStore is evidence of a bug in the
// harness or in the checker, never in the store, and it should be treated that
// way before anything else is investigated.
//
// What it is not is available. There is one node, so cutting it off does not
// produce wrong answers, it produces no answers.
type SingleStore struct {
	mu   sync.Mutex
	data map[string]int
}

// NewSingleStore returns an empty store. Every key starts at
// model.InitialValue, so a read before any write is still an assertion about
// the register rather than a special case.
func NewSingleStore() *SingleStore {
	return &SingleStore{data: map[string]int{}}
}

// Apply performs one operation under the store's lock.
//
// It never returns an error. There is no network between the decision and the
// data, so this store always knows what happened, and that certainty is
// exactly what makes it the control case.
func (s *SingleStore) Apply(ctx context.Context, req Request) (Response, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	switch req.Kind {
	case history.Write:
		s.data[req.Key] = req.Value
		return WriteOK(), nil

	case history.CAS:
		if s.get(req.Key) != req.From {
			// Not a refusal. The server compared, found a different value and
			// correctly did nothing, which is a successful compare-and-swap
			// that reports it did not swap.
			return CASOK(false), nil
		}
		s.data[req.Key] = req.To
		return CASOK(true), nil

	default:
		return ReadOK(s.get(req.Key)), nil
	}
}

// Configure accepts and ignores late configuration. A single node has no peers
// to be told about, but it answers /configure so the harness can wire all
// three stores the same way.
func (s *SingleStore) Configure(cfg Config) error {
	_ = cfg
	return nil
}

// Close does nothing; the store starts no background work.
func (s *SingleStore) Close() error { return nil }

// get reads a key, supplying the initial value for one never written. The
// caller holds the lock.
func (s *SingleStore) get(key string) int {
	if v, ok := s.data[key]; ok {
		return v
	}
	return model.InitialValue
}

var _ Store = (*SingleStore)(nil)
