package main

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestActivityComparisonOrder(t *testing.T) {
	random := rand.New(rand.NewPCG(41, 19))
	agg := aggregator{}
	for _, count := range []int{0, 1, 16, 17, 127, 4095} {
		for _, pattern := range []string{"sorted", "reversed", "ties", "random"} {
			original := make([]activity, count)
			for i := range original {
				ts := int64(i)
				switch pattern {
				case "reversed":
					ts = -ts
				case "ties":
					ts = random.Int64N(3) - 1
				case "random":
					ts = random.Int64()
				}
				original[i] = activity{ts: ts, cents: uint32(i)}
			}
			rows := agg.sortActivity(slices.Clone(original))
			if !slices.IsSortedFunc(rows, func(a, b activity) int { return cmp.Compare(a.ts, b.ts) }) {
				t.Fatalf("count %d, %s: timestamps are out of order", count, pattern)
			}
			slices.SortFunc(rows, func(a, b activity) int { return cmp.Compare(a.cents, b.cents) })
			if !slices.Equal(rows, original) {
				t.Fatalf("count %d, %s: sort did not preserve payloads", count, pattern)
			}
		}
	}
}

func TestActivityRadixOrder(t *testing.T) {
	random := rand.New(rand.NewPCG(21, 42))
	agg := aggregator{}
	for _, limit := range []int64{1, 256, 1 << 19, 1 << 35, math.MaxInt64} {
		rows := make([]activity, 8192)
		for i := range rows {
			rows[i] = activity{ts: random.Int64N(limit) - limit/2, cents: uint32(i)}
		}
		if limit == math.MaxInt64 {
			rows[0].ts, rows[1].ts = math.MinInt64, math.MaxInt64
		}
		expected := slices.Clone(rows)
		slices.SortStableFunc(expected, func(a, b activity) int { return cmp.Compare(a.ts, b.ts) })
		if got := agg.sortActivity(rows); !slices.Equal(got, expected) {
			t.Fatalf("limit %d: radix order differs from comparison sort", limit)
		}
	}
}
