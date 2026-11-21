package main

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

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
