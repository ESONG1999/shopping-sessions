package main

import (
	"cmp"
	"math/bits"
	"slices"
)

func (agg *aggregator) sortActivity(rows []uint64) []uint64 {
	if agg.codes.wide != nil && slices.ContainsFunc(rows, func(row uint64) bool { return row&wideActivity != 0 }) {
		slices.SortFunc(rows, func(a, b uint64) int { return cmp.Compare(agg.codes.timestamp(a), agg.codes.timestamp(b)) })
		return rows
	}
	if len(rows) < 4096 {
		slices.Sort(rows)
		return rows
	}
	// Heavy users benefit from radix passes over the active timestamp
	// range. Subtracting the minimum also handles negative Unix times.
	earliest, latest := rows[0]>>32, rows[0]>>32
	for _, row := range rows[1:] {
		earliest, latest = min(earliest, row>>32), max(latest, row>>32)
	}
	width := bits.Len64(latest - earliest)
	if width == 0 {
		return rows
	}
	if cap(agg.timeScratch) < len(rows) {
		agg.timeScratch = make([]uint64, len(rows))
	}
	src, dst := rows, agg.timeScratch[:len(rows)]
	const digitBits = 11
	for shift := 0; shift < width; shift += digitBits {
		var positions [1 << digitBits]int
		count := 1 << min(digitBits, width-shift)
		mask := uint64(count - 1)
		for _, row := range src {
			digit := ((row>>32 - earliest) >> shift) & mask
			positions[digit]++
		}
		offset := 0
		for i := range positions[:count] {
			positions[i], offset = offset, offset+positions[i]
		}
		for _, row := range src {
			digit := ((row>>32 - earliest) >> shift) & mask
			dst[positions[digit]] = row
			positions[digit]++
		}
		src, dst = dst, src
	}
	return src
}
