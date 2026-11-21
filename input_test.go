package main

import (
	"strings"
	"testing"
)

func TestInputChunksOwnRecordsExactlyOnce(t *testing.T) {
	for _, text := range []string{
		"",
		"header without newline",
		"header\n",
		"header\nfirst,row\nsecond,row",
		"long header with commas,,,,,,,,,\n\n\nshort\n" + strings.Repeat("long record,", 50) + "\nlast",
	} {
		_, want, _ := strings.Cut(text, "\n")
		for _, size := range []int{1, 2, 7, 8, 31, 64} {
			var got strings.Builder
			for start := 0; start < len(text); start += size {
				chunk := inputChunk{text, start, min(start+size, len(text))}
				got.WriteString(chunk.records())
			}
			if got.String() != want {
				t.Fatalf("chunk size %d: got %q, want %q", size, got.String(), want)
			}
		}
	}
}
