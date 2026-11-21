package main

import "testing"

func TestCompactCandidatePreservesActivityAndIngestOrder(t *testing.T) {
	const start = -1_000_000
	input := candidateInput{
		byKey: make([]partition[candidate], 1),
		wide:  make([][]activity, 1),
		start: start,
	}
	var previous uint64
	for i, tc := range []struct {
		ingest int64
		row    activity
	}{
		{-62_167_219_200_000, activity{ts: start}},
		{-1000, activity{ts: start + (1 << candidateTimeBits) - 1, cents: 10_000_000, category: 65535}},
		{0, activity{ts: start + (1 << candidateTimeBits), cents: 18, category: 65535}},
		{253_402_300_799_000, activity{ts: start + 101, cents: 76, category: 65536}},
	} {
		input.add(0, 1, tc.ingest, event{person: 1, activity: tc.row})
		got := input.byKey[0][1].rows[0][i]
		if decoded := input.activity(got); decoded != tc.row {
			t.Fatalf("candidate %d: got %#v, want %#v", i, decoded, tc.row)
		}
		order := got.ingestOrder()
		if order <= previous {
			t.Fatalf("candidate %d: ingest order %d followed %d", i, order, previous)
		}
		previous = order
	}
}
