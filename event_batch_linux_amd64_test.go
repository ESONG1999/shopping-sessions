//go:build nativecsv && cgo

package main

import (
	"strings"
	"testing"
	"time"
)

func TestNativeEventBatchMatchesGo(t *testing.T) {
	window := window{
		start: time.Date(2028, 2, 28, 0, 0, 0, 0, time.UTC).UnixMilli(),
		end:   time.Date(2028, 3, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
	}
	catalog := catalog{products: map[string]uint32{"p1": 1}, unknown: 2}
	rows := []string{
		`e1234567,u12345678,2028-02-29T00:00:00Z,2028-03-01T00:00:00Z,cart,p1,x`,
		`e1234567,u12345678,2028-02-29T00:00:00Z,2028-03-01T00:00:01Z,cart,p1,x`,
		`"é","user longer than nine",2028-02-28T23:30:00-00:30,2028-03-01T00:00:00Z,purchase,"p1",0250`,
		`"has "" quotes",u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,cart,p1,`,
		`e1,u1,1835395200000,2028-02-29T00:00:00Z,view,p1,`,
		`e1,u1,18353952x0000,2028-02-29T00:00:00Z,view,p1,`,
		`e2,u1,0001835395200000,2028-02-29T00:00:00Z,view, p1 ,`,
		`e2,u1,2028-02-30T00:00:00Z,2028-03-01T00:00:00Z,cart,p1,`,
		`e2,u1,2028-02-29T00:00:00+24:00,2028-02-29T00:00:00Z,cart,p1,`,
		`e2,u1,2028-02-29T00:00:00Z,2028-02-30T00:00:00Z,cart,p1,`,
		`e3,u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,unknown,10000000`,
		`e4,u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,p1,10000001`,
		`e5,u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,view,"",`,
		`e6,u1,18446744073709551615,2028-02-29T00:00:00Z,cart,p1,`,
		`e7,u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,` + strings.Repeat("p", 256) + ",12",
		`e8,"",2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,cart,p1,`,
		"e9,u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,p1,12\r",
		`eA,u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,cart,p1,,`,
		`eB,u1,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,unknown,`,
		"",
	}

	// Check alignment at all SIMD lane offsets, several output batch
	// boundaries, and the last record without a newline.
	for offset := range 32 {
		input := strings.Repeat("x", offset) + "\n" + strings.Repeat(strings.Join(rows, "\n")+"\n", 75) + rows[0]
		var want, got []inputEvent
		var decoder timestampDecoder
		ids := &identities{}
		scanCSV(input, 7, func(f []string) {
			if row := parseEvent(&decoder, f, window, ids, catalog); row.key != 0 {
				want = append(want, row)
			}
		})
		if !scanNativeEvents(input, window, ids, catalog, func(row inputEvent) {
			got = append(got, row)
		}) {
			t.Skip("AVX2 event parser unavailable")
		}
		if len(got) != len(want) {
			t.Fatalf("offset %d: got %d event candidates, want %d", offset, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("offset %d, candidate %d: got %+v, want %+v", offset, i, got[i], want[i])
			}
		}
	}
}
