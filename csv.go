package main

import (
	"encoding/binary"
	"math/bits"
)

const csvBlock = 32

type csvMask struct {
	separators uint32
	quotes     uint32
}

func scanCSV(text string, width int, visit func([]string)) {
	var values [7]string
	record := values[:width]
	var masks [128]csvMask
	start, column, carriedQuotes := 0, 0, 0
	malformed := false

	// Use the same field checks for the real newline and for an unterminated
	// final row. Commas always separate fields, including on malformed rows.
	field := func(end, quotes int, newline bool) {
		if !malformed {
			value := text[start:end]
			switch {
			case column == width:
				malformed = true
			case quotes == 0:
				record[column] = value
			case quotes == 2 && value[0] == '"' && value[len(value)-1] == '"':
				record[column] = value[1 : len(value)-1]
			default:
				malformed = true
			}
		}
		column++
		start = end + 1
		if newline {
			if !malformed && column == width {
				visit(record)
			}
			column, malformed = 0, false
		}
	}

	for offset := 0; offset < len(text); {
		wholeBlocks := min(len(masks), (len(text)-offset)/csvBlock)
		n := wholeBlocks
		if wholeBlocks > 0 {
			fillCSVMasks(text[offset:offset+wholeBlocks*csvBlock], masks[:wholeBlocks])
		} else {
			masks[0] = tailCSVMask(text[offset:])
			n = 1
		}
		for _, mask := range masks[:n] {
			separators, quotes := mask.separators, mask.quotes
			for separators != 0 {
				next := bits.TrailingZeros32(separators)
				before := uint32(1)<<next - 1
				count := carriedQuotes + bits.OnesCount32(quotes&before)
				end := offset + next
				field(end, count, text[end] == '\n')
				separators &= separators - 1
				quotes &^= before
				carriedQuotes = 0
			}
			carriedQuotes += bits.OnesCount32(quotes)
			offset += csvBlock
		}
	}
	if start < len(text) || column != 0 {
		field(len(text), carriedQuotes, true)
	}
}

func tailCSVMask(text string) csvMask {
	var mask csvMask
	for i := 0; i < len(text); i++ {
		bit := uint32(1) << i
		switch text[i] {
		case ',', '\n':
			mask.separators |= bit
		case '"':
			mask.quotes |= bit
		}
	}
	return mask
}

// Compare eight bytes in parallel and pack the comparison into eight bits.
func byteMatches(word, repeatedByte uint64) uint32 {
	const low = 0x7f7f7f7f7f7f7f7f
	x := word ^ repeatedByte
	highBits := ^(x | ((x & low) + low)) & 0x8080808080808080
	return uint32(((highBits >> 7) * 0x0102040810204080) >> 56)
}

func fillCSVMasksPortable(text string, masks []csvMask) {
	const spread = 0x0101010101010101
	for i := range masks {
		var separators, quotes uint32
		block := text[i*csvBlock : (i+1)*csvBlock]
		for j := 0; j < csvBlock; j += 8 {
			word := binary.LittleEndian.Uint64([]byte(block[j : j+8]))
			separators |= (byteMatches(word, spread*',') | byteMatches(word, spread*'\n')) << j
			quotes |= byteMatches(word, spread*'"') << j
		}
		masks[i] = csvMask{separators, quotes}
	}
}
