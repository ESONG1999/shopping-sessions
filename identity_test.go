package main

import (
	"math/rand/v2"
	"testing"
)

func TestIdentityPacking(t *testing.T) {
	ids := &identities{}
	random := rand.New(rand.NewPCG(34, 9))
	for _, length := range []int{8, 9} {
		for n := 0; n < 2048; n++ {
			raw := make([]byte, length)
			var expected uint64
			for i := range raw {
				raw[i] = byte(1 + random.IntN(127))
				expected = expected<<7 | uint64(raw[i])
			}
			want := expected << (7 * (packedBytes - length))
			if got := ids.key(string(raw)); got != want {
				t.Fatalf("key(%q) = %x, want %x", raw, got, want)
			}
		}
	}
	for _, id := range []string{"abc\x00defg", "abcdefgh\x00", "abcédefg", "long identifier"} {
		key := ids.key(id)
		if key&externalID == 0 || ids.name(key) != id {
			t.Fatalf("lossy identity encoding for %q", id)
		}
	}
}

func TestMixedIdentityRoundTrip(t *testing.T) {
	random := rand.New(rand.NewPCG(81, 36))
	for i := 0; i < 8192; i++ {
		key := random.Uint64()
		if got := unmix(mix(key)); got != key {
			t.Fatalf("round trip for %016x returned %016x", key, got)
		}
	}
}
