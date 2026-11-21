package main

import (
	"cmp"
	"math/bits"
	"slices"
)

func (agg *aggregator) sortActivity(rows []activity) []activity {
	if len(rows) < 4096 {
		slices.SortFunc(rows, func(a, b activity) int { return cmp.Compare(a.ts, b.ts) })
		return rows
	}
	// Heavy users benefit from radix passes over the active timestamp
	// range. Subtracting the minimum also handles negative Unix times.
	earliest, latest := rows[0].ts, rows[0].ts
	for _, row := range rows[1:] {
		earliest, latest = min(earliest, row.ts), max(latest, row.ts)
	}
	base := uint64(earliest)
	width := bits.Len64(uint64(latest) - base)
	if width == 0 {
		return rows
	}
	if cap(agg.timeScratch) < len(rows) {
		agg.timeScratch = make([]activity, len(rows))
	}
	src, dst := rows, agg.timeScratch[:len(rows)]
	const digitBits = 11
	for shift := 0; shift < width; shift += digitBits {
		var positions [1 << digitBits]int
		count := 1 << min(digitBits, width-shift)
		mask := uint64(count - 1)
		for _, row := range src {
			digit := ((uint64(row.ts) - base) >> shift) & mask
			positions[digit]++
		}
		offset := 0
		for i := range positions[:count] {
			positions[i], offset = offset, offset+positions[i]
		}
		for _, row := range src {
			digit := ((uint64(row.ts) - base) >> shift) & mask
			dst[positions[digit]] = row
			positions[digit]++
		}
		src, dst = dst, src
	}
	return src
}
