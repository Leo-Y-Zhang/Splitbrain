package kv

import (
	"strings"
	"testing"
)

func TestDecodeRequestRefusesANoOpCAS(t *testing.T) {
	// history.Validate rejects a compare-and-swap from a value to itself, so a
	// server that cheerfully answers one produces a history its own repository
	// cannot check. Reproduced by hand before this existed:
	//   POST /kv {"op":"cas","key":"k","from":5,"to":5}
	//   -> 200 {"ok":true,"swapped":false}
	// Two halves of one tool disagreeing about what an operation is, is worse
	// than either rule on its own.
	if _, err := decodeRequest(strings.NewReader(`{"op":"cas","key":"k","from":5,"to":5}`)); err == nil {
		t.Fatal("the server accepted a cas that goes nowhere; history.Validate refuses that history")
	}

	// A real one still works.
	req, err := decodeRequest(strings.NewReader(`{"op":"cas","key":"k","from":5,"to":6}`))
	if err != nil {
		t.Fatalf("a genuine cas was refused: %v", err)
	}
	if req.From != 5 || req.To != 6 {
		t.Fatalf("decoded %+v", req)
	}
}
