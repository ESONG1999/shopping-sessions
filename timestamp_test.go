package main

import (
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEpochMillisMatchesScalar(t *testing.T) {
	random := rand.New(rand.NewPCG(33, 54))
	for length := 1; length <= 25; length++ {
		for i := 0; i < 100; i++ {
			raw := make([]byte, length)
			for j := range raw {
				raw[j] = '0' + byte(random.IntN(10))
			}
			if i%3 == 0 {
				raw[random.IntN(length)] = byte(random.IntN(256))
			}
			s := string(raw)
			want, valid := asciiNumber(s, math.MaxInt64)
			if got, ok := epochMillis(s); ok != valid || got != want {
				t.Fatalf("epochMillis(%q) = %d, %v, want %d, %v", s, got, ok, want, valid)
			}
		}
	}
}

func TestTimestampDecoderCalendar(t *testing.T) {
	var decoder timestampDecoder
	for _, year := range []int{0, 1, 1900, 1969, 2000, 2026, 2028, 9999} {
		for month := time.January; month <= time.December; month++ {
			zone := time.FixedZone("test", -3*3600-45*60)
			for _, ts := range []time.Time{
				time.Date(year, month, 1, 0, 0, 0, 0, time.UTC),
				time.Date(year, month+1, 0, 23, 31, 59, 0, zone),
			} {
				formatted := ts.Format(time.RFC3339)
				if got, ok := decoder.timestamp(formatted); !ok || got != ts.UnixMilli() {
					t.Fatalf("timestamp(%q) = %d, %v, want %d", formatted, got, ok, ts.UnixMilli())
				}
			}
		}
	}
	for _, s := range []string{
		"2026-02-29T03:00:01Z", "2028-02-30T03:00:01Z", "2028-02-29T24:00:01Z",
		"2028-02-29T03:00:60Z", "2028-02-29T03:00:01+24:00", "2028-02-29T03:00:01+23:60",
		"2028-02-29T03:00:01z", "2028x02-29T03:00:01Z", "2028-02-29t03:00:01Z",
	} {
		if _, ok := decoder.timestamp(s); ok {
			t.Fatalf("accepted malformed timestamp %q", s)
		}
	}
	padded := strings.Repeat("0", 22) + strconv.FormatInt(math.MaxInt64, 10)
	if got, ok := decoder.timestamp(padded); !ok || got != math.MaxInt64 {
		t.Fatalf("timestamp with leading zeros = %d, %v", got, ok)
	}
}
