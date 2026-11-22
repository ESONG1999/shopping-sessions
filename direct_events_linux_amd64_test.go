//go:build nativecsv && cgo

package main

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"
)

func TestDirectEventPartitionsMatchGo(t *testing.T) {
	if !nativeEventDecoder() {
		t.Skip("AVX2 event decoder unavailable")
	}
	start := time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	w := window{start, start + 40*dayMillis}
	ids := &identities{}
	cat := catalog{products: map[string]uint32{"p1": 1}, unknown: 2}
	source, canonical := mix(ids.key("u2")), mix(ids.key("u1"))
	a := aliases{entries: make(aliasTable, 2), filter: []uint64{aliasBits(source)}}
	a.entries[a.entries.slot(source, 0)] = aliasEntry{source, canonical}

	var first strings.Builder
	for i := range 2*blockSize + 3 {
		fmt.Fprintf(&first, "e%07d,u2,2028-01-01T02:00:00Z,2028-01-01T02:01:00Z,cart,p1,\n", i)
	}
	first.WriteString("some-long-event-id,another-long-user,2028-01-01T02:00:00Z,2028-01-01T02:01:00Z,cart,p1,\n")
	second := strings.Join([]string{
		"e0000000,u1,invalid,2028-01-01T01:01:00Z,cart,p1,",
		"e0000001,u1,2028-01-01T01:00:00Z,2028-01-01T01:01:00Z,purchase,p1,123",
		"some-long-event-id,u1,2028-01-01T01:00:00Z,2028-01-01T01:01:00Z,view,p1,",
		"wide,u2,2028-01-30T02:00:00Z,2028-01-30T02:01:00Z,view,p1,",
		"last,u1,2028-01-01T03:00:00Z,2028-01-01T03:01:00Z,view,p1,",
	}, "\n")
	parts := []string{first.String(), second}

	type personActivity struct {
		person uint64
		activity
	}
	run := func(fused, tight bool) map[personActivity]int {
		raw := eventInput{lanes: make([]eventLane, 1), base: w.start/1000 - ingestBias}
		defer raw.close()
		lane := &raw.lanes[0]
		lane.activityStart = w.start
		var native []directEventLane
		if fused {
			native = allocateDirectLanes(mappedChunks{mappings: [][]byte{[]byte(parts[0]), []byte(parts[1])}}, 1)
			if native == nil {
				t.Fatal("direct arena allocation failed")
			}
			if tight {
				native[0].keyOwners = native[0].keyOwners[:1]
			}
		}
		for i, text := range parts {
			if i != 0 {
				lane.aliases = &a
			}
			if fused && native[0].scan(text, w, ids, cat, lane, raw.base) {
				continue
			}
			var decoder timestampDecoder
			scanCSV(text, 7, func(fields []string) {
				if row := parseEvent(&decoder, fields, w, ids, cat); row.key != 0 {
					lane.add(row, raw.base)
				}
			})
		}
		if fused {
			if blocks := native[0].state.EventBlocks; blocks == 0 || (!tight && blocks < 3) {
				t.Fatalf("fused path allocated %d event blocks", blocks)
			}
			native[0].publish(&raw, 0)
		}
		result := deduplicate(&raw, 1, a)
		codes := raw.activityCodes(w.start)
		out := make(map[personActivity]int)
		for _, bucket := range result[0].buckets {
			for _, block := range bucket.rows {
				for _, event := range block {
					if event.person != 0 {
						out[personActivity{event.person, codes.unpack(event.payload)}]++
					}
				}
			}
		}
		return out
	}
	want := run(false, false)
	for _, tight := range []bool{false, true} {
		if got := run(true, tight); !maps.Equal(got, want) {
			t.Fatalf("direct and Go survivors differ (limited arena %v): got %v, want %v", tight, got, want)
		}
	}
}
