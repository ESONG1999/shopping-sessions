package main

import (
	"encoding/binary"
	"math/bits"
)

const csvBlock = 32

type csvMask struct {
	separators uint32
	quotes     uint32
	newlines   uint32
}

func scanCSV(text string, width int, visit func([]string)) {
	var values [7]string
	var ends [7]int
	record := values[:width]
	var masks [128]csvMask
	start, column, rowQuotes := 0, 0, 0

	for offset := 0; offset <= len(text); {
		wholeBlocks := min(len(masks), (len(text)-offset)/csvBlock)
		n := wholeBlocks
		if wholeBlocks > 0 {
			fillCSVMasks(text[offset:offset+wholeBlocks*csvBlock], masks[:wholeBlocks])
		} else {
			masks[0] = tailCSVMask(text[offset:])
			// Treat an unterminated final record like any other record.
			if len(text) != 0 && text[len(text)-1] != '\n' {
				bit := uint32(1) << (len(text) - offset)
				masks[0].separators |= bit
				masks[0].newlines |= bit
			}
			n = 1
		}
		for _, mask := range masks[:n] {
			commas, quotes := mask.separators&^mask.newlines, mask.quotes
			for newlines := mask.newlines; newlines != 0; newlines &= newlines - 1 {
				newline := newlines & -newlines
				below := newline - 1
				end := offset + bits.TrailingZeros32(newlines)
				fields := commas & below
				if column+bits.OnesCount32(fields) == width-1 {
					csvFieldEnds(ends[column:width-1], offset, fields)
					ends[width-1] = end
					rowQuotes += bits.OnesCount32(quotes & below)
					if decodeCSVRecord(text, start, ends[:width], record, rowQuotes) {
						visit(record)
					}
				}
				commas &^= below
				quotes &^= below
				start, column, rowQuotes = end+1, 0, 0
			}
			if commas != 0 {
				last := column + bits.OnesCount32(commas)
				if last < width {
					csvFieldEnds(ends[column:last], offset, commas)
				}
				column = min(last, width)
			}
			rowQuotes += bits.OnesCount32(quotes)
			offset += csvBlock
		}
	}
}

func csvFieldEnds(ends []int, offset int, commas uint32) {
	for i := range ends {
		ends[i] = offset + bits.TrailingZeros32(commas)
		commas &= commas - 1
	}
}

func decodeCSVRecord(text string, start int, ends []int, record []string, quotes int) bool {
	if quotes == 0 {
		for i, end := range ends {
			record[i] = text[start:end]
			start = end + 1
		}
		return true
	}
	// Each wrapped field accounts for exactly two known quote bytes.
	// Any unmatched quote must be outside the permitted wrapper positions.
	wrapped := 0
	for i, end := range ends {
		value := text[start:end]
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
			wrapped++
		}
		record[i] = value
		start = end + 1
	}
	return quotes == 2*wrapped
}

func tailCSVMask(text string) csvMask {
	var mask csvMask
	for i := 0; i < len(text); i++ {
		bit := uint32(1) << i
		switch text[i] {
		case ',':
			mask.separators |= bit
		case '\n':
			mask.separators |= bit
			mask.newlines |= bit
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
		var commas, quotes, newlines uint32
		block := text[i*csvBlock : (i+1)*csvBlock]
		for j := 0; j < csvBlock; j += 8 {
			word := binary.LittleEndian.Uint64([]byte(block[j : j+8]))
			commas |= byteMatches(word, spread*',') << j
			quotes |= byteMatches(word, spread*'"') << j
			newlines |= byteMatches(word, spread*'\n') << j
		}
		masks[i] = csvMask{commas | newlines, quotes, newlines}
	}
}
