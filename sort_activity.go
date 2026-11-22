package main

import (
	"cmp"
	"math/bits"
	"slices"
)

func (agg *aggregator) sortActivity(rows []activity) []activity {
	if len(rows) < 4096 {
		sortSmallActivity(rows, 2*bits.Len(uint(len(rows))))
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

// Keep timestamp comparisons inline for the histories that fit in cache.
func sortSmallActivity(rows []activity, depth int) {
	for len(rows) > 16 {
		if depth == 0 {
			slices.SortFunc(rows, func(a, b activity) int { return cmp.Compare(a.ts, b.ts) })
			return
		}
		depth--
		a, b, c := rows[0].ts, rows[len(rows)/2].ts, rows[len(rows)-1].ts
		pivot := max(min(a, b), min(max(a, b), c))
		left, right := 0, len(rows)-1
		for left <= right {
			for rows[left].ts < pivot {
				left++
			}
			for rows[right].ts > pivot {
				right--
			}
			if left <= right {
				rows[left], rows[right] = rows[right], rows[left]
				left++
				right--
			}
		}
		// Recurse on the smaller side to bound stack use.
		if right+1 < len(rows)-left {
			sortSmallActivity(rows[:right+1], depth)
			rows = rows[left:]
		} else {
			sortSmallActivity(rows[left:], depth)
			rows = rows[:right+1]
		}
	}
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		j := i
		for j > 0 && rows[j-1].ts > row.ts {
			rows[j] = rows[j-1]
			j--
		}
		rows[j] = row
	}
}
