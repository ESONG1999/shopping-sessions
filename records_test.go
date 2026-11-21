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
		input.add(0, 1, tc.ingest, 1, tc.row)
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

func TestDedupKeepsDistinctIDsWithTheSameCounter(t *testing.T) {
	input := candidateInput{
		byKey: make([]partition[candidate], 1),
		wide:  make([][]activity, 1),
	}
	// These keys share the same two-bit counter and fingerprint.
	const one = uint64(1)
	const two = one | 1<<40
	const three = one | 2<<40
	input.add(0, one, 5000, 1, activity{ts: 1})
	input.add(0, two, 1000, 2, activity{ts: 2})
	input.add(0, one, 2000, 0, activity{})
	input.add(0, three, 4000, 3, activity{ts: 11})
	input.add(0, three, 1000, 3, activity{ts: 3})
	out := deduplicate(input, 1, aliases{})
	got := make(map[uint64]int64)
	for _, bucket := range out[0] {
		for _, block := range bucket.rows {
			for _, row := range block {
				person := unmix(row.person)
				if _, exists := got[person]; exists {
					t.Fatalf("duplicate emitted for person %d", person)
				}
				got[person] = row.ts
			}
		}
	}
	if len(got) != 2 || got[2] != 2 || got[3] != 3 {
		t.Fatalf("wrong survivors: %v", got)
	}
}
