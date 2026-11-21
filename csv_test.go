package main

import (
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func TestBatchedCSVMatchesScalar(t *testing.T) {
	random := rand.New(rand.NewPCG(7, 12))
	for _, width := range []int{2, 4, 7} {
		var input strings.Builder
		for row := 0; row < 800; row++ {
			columns := width
			if row%5 == 0 {
				columns += random.IntN(3) - 1
			}
			for col := 0; col < columns; col++ {
				if col != 0 {
					input.WriteByte(',')
				}
				value := strings.Repeat(" é\r", random.IntN(35))
				switch random.IntN(5) {
				case 0:
					value = `"` + value + `"`
				case 1:
					value = `"` + value
				case 2:
					value = value + `""`
				}
				input.WriteString(value)
			}
			if row < 799 {
				input.WriteByte('\n')
			}
		}
		var expected, actual [][]string
		lines(input.String(), func(line string) {
			record := make([]string, width)
			if fields(line, record) {
				expected = append(expected, record)
			}
		})
		scanCSV(input.String(), width, func(record []string) {
			actual = append(actual, slices.Clone(record))
		})
		if len(actual) != len(expected) {
			t.Fatalf("width %d: got %d valid rows, want %d", width, len(actual), len(expected))
		}
		for i := range actual {
			if !slices.Equal(actual[i], expected[i]) {
				t.Fatalf("width %d, row %d: got %q, want %q", width, i, actual[i], expected[i])
			}
		}
	}
}
