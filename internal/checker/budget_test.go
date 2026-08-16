package checker

import (
	"strings"
	"testing"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// wideConcurrentHistory builds a history on one key that the search has to work
// at: every operation overlaps a long-running write, so almost any ordering is
// a candidate and the cache fills.
func wideConcurrentHistory(n int) history.History {
	h := history.History{
		{Process: 0, Key: "k0", Kind: history.Write, Value: 1,
			Outcome: history.OK, Invoke: 0, Complete: int64(n) * 10},
	}
	for i := 1; i <= n; i++ {
		t := int64(i)
		h = append(h, history.Op{
			Process: i, Key: "k0", Kind: history.Write, Value: i + 1,
			Outcome: history.OK, Invoke: t, Complete: int64(n)*10 - 1,
		})
	}
	return h
}

func TestCacheBudgetYieldsUnknownRatherThanMemory(t *testing.T) {
	// The visit budget counts model transitions, and each one can add a cache
	// entry as wide as the key has operations, so it does not bound memory at
	// all. Before this ceiling existed a twelve-megabyte history took 4.96 GB
	// of resident memory under the default flags and then reported Unknown
	// anyway. This is that same verdict, reached before the memory is spent.
	h := wideConcurrentHistory(400)

	res, err := Check(h, model.CASRegister{}, Options{MaxCacheBytes: 8 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != Unknown {
		t.Fatalf("verdict %s with an 8 KB cache ceiling, want unknown", res.Verdict)
	}
	if res.Verdict == Linearizable {
		t.Fatal("a search stopped by its memory ceiling was reported as a pass")
	}
	if !strings.Contains(res.Reason, "memory budget") {
		t.Fatalf("reason %q does not say which budget ran out", res.Reason)
	}
	t.Logf("%s", res.Reason)
}

func TestCacheBudgetDoesNotFireWhenItIsRoomy(t *testing.T) {
	// The ceiling must not turn ordinary histories into Unknown, or the tool
	// becomes useless in the safe direction instead of the unsafe one.
	h := wideConcurrentHistory(400)

	res, err := Check(h, model.CASRegister{}, Options{MaxCacheBytes: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict == Unknown {
		t.Fatalf("a 256 MB ceiling stopped a %d-operation history: %s", len(h), res.Reason)
	}
	t.Logf("%s in %s, %d states", res.Verdict, res.Elapsed, res.Visits)
}

func TestCacheBudgetOfZeroMeansUnlimited(t *testing.T) {
	// Consistent with MaxVisits, and the reason the command line sets a real
	// default rather than relying on the zero value.
	h := wideConcurrentHistory(60)
	res, err := Check(h, model.CASRegister{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict == Unknown {
		t.Fatalf("an unlimited cache still gave up: %s", res.Reason)
	}
}

func TestCacheAccountsForTheWidthOfWhatItStores(t *testing.T) {
	// A cache entry for a history with a thousand operations costs far more
	// than one for a history with ten, and a ceiling that ignored that would
	// be a ceiling on the wrong quantity.
	narrow := newCache(1)
	wide := newCache(64)
	if narrow.entryBytes >= wide.entryBytes {
		t.Fatalf("entry cost does not grow with width: narrow %d, wide %d", narrow.entryBytes, wide.entryBytes)
	}

	b := newBitset(64 * 64)
	before := wide.Bytes()
	wide.add(b, 0)
	if got := wide.Bytes() - before; got != wide.entryBytes {
		t.Fatalf("adding one entry moved the estimate by %d, want %d", got, wide.entryBytes)
	}
}

func TestHumanBytes(t *testing.T) {
	// The reason line is read by people, and "536870912" is not a number
	// anybody parses at a glance.
	for in, want := range map[int]string{
		512:               "512 bytes",
		2048:              "2 KB",
		5 << 20:           "5 MB",
		3 * (1 << 30):     "3.0 GB",
		(1 << 30) * 3 / 2: "1.5 GB",
	} {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}
