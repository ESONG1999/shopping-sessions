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
	for _, count := range []int{0, 1, 16, 17, 127, 4095} {
		for _, pattern := range []string{"sorted", "reversed", "ties", "random"} {
			lane := eventLane{activityStart: -1 << 30}
			original := make([]activity, count)
			for i := range original {
				ts := int64(i)
				switch pattern {
				case "reversed":
					ts = -ts
				case "ties":
					ts = random.Int64N(3) - 1
				case "random":
					ts = random.Int64N(1<<31) - 1<<30
				}
				original[i] = activity{ts: ts, cents: uint32(i)}
			}
			var raw eventInput
			rows := make([]uint64, len(original))
			for i, row := range original {
				rows[i] = lane.packActivity(row)
			}
			raw.lanes = []eventLane{lane}
			agg := aggregator{codes: raw.activityCodes(lane.activityStart)}
			sorted := agg.sortActivity(rows)
			decoded := make([]activity, len(sorted))
			for i, row := range sorted {
				decoded[i] = agg.codes.unpack(row)
			}
			if !slices.IsSortedFunc(decoded, func(a, b activity) int { return cmp.Compare(a.ts, b.ts) }) {
				t.Fatalf("count %d, %s: timestamps are out of order", count, pattern)
			}
			slices.SortFunc(decoded, func(a, b activity) int { return cmp.Compare(a.cents, b.cents) })
			if !slices.Equal(decoded, original) {
				t.Fatalf("count %d, %s: sort did not preserve payloads", count, pattern)
			}
		}
	}
}

func TestActivityRadixOrder(t *testing.T) {
	random := rand.New(rand.NewPCG(21, 42))
	lane := eventLane{activityStart: -1 << 30}
	for _, limit := range []int64{1, 256, 1 << 19, 1 << 31, 1 << 35, math.MaxInt64} {
		lane.wideActivities = nil
		rows := make([]activity, 8192)
		for i := range rows {
			rows[i] = activity{ts: random.Int64N(limit) - limit/2, cents: uint32(i)}
		}
		if limit == math.MaxInt64 {
			rows[0].ts, rows[1].ts = math.MinInt64, math.MaxInt64
		}
		expected := slices.Clone(rows)
		slices.SortStableFunc(expected, func(a, b activity) int { return cmp.Compare(a.ts, b.ts) })
		packed := make([]uint64, len(rows))
		for i, row := range rows {
			packed[i] = lane.packActivity(row)
		}
		raw := eventInput{lanes: []eventLane{lane}}
		agg := aggregator{codes: raw.activityCodes(lane.activityStart)}
		got := agg.sortActivity(packed)
		for i, value := range got {
			rows[i] = agg.codes.unpack(value)
		}
		// Compare the timestamp order. Numeric sort can reorder equal timestamps.
		slices.SortFunc(rows, func(a, b activity) int {
			if a.ts == b.ts {
				return cmp.Compare(a.cents, b.cents)
			}
			return cmp.Compare(a.ts, b.ts)
		})
		if !slices.IsSortedFunc(got, func(a, b uint64) int { return cmp.Compare(agg.codes.timestamp(a), agg.codes.timestamp(b)) }) || !slices.Equal(rows, expected) {
			t.Fatalf("limit %d: radix order differs from comparison sort", limit)
		}
	}
}

func TestActivityCodeOverflowAcrossLanes(t *testing.T) {
	const start = int64(-2000)
	raw := eventInput{lanes: []eventLane{{activityStart: start}, {activityStart: start, worker: 1}}}
	inputs := []activity{
		{ts: start, cents: 125, category: 0},
		{ts: start + 1<<31 - 1, cents: 10_000_000, category: 255},
		{ts: start + 1<<31, cents: 127, category: 1},
		{ts: start + 19, cents: 129, category: 256},
	}
	rows := make([]uint64, len(inputs))
	for i, row := range inputs {
		rows[i] = raw.lanes[i%2].packActivity(row)
	}
	codes := raw.activityCodes(start)
	for i, row := range rows {
		if got := codes.unpack(row); got != inputs[i] || codes.timestamp(row) != inputs[i].ts {
			t.Fatalf("row %d: decoded %+v, want %+v", i, got, inputs[i])
		}
	}
	if rows[0]&wideActivity != 0 || rows[1]&wideActivity != 0 || rows[2]&wideActivity == 0 || rows[3]&wideActivity == 0 {
		t.Fatalf("wrong representation at encoding limits")
	}
}
