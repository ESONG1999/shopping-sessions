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
	record := values[:width]
	var masks [128]csvMask
	start, column, carriedQuotes := 0, 0, 0
	malformed := false

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
			separators, quotes := mask.separators, mask.quotes
			for separators != 0 {
				bit := separators & -separators
				seenQuotes := quotes & (bit - 1)
				end := offset + bits.TrailingZeros32(separators)
				if column < width {
					value := text[start:end]
					if count := carriedQuotes + bits.OnesCount32(seenQuotes); count != 0 {
						if count == 2 && value[0] == '"' && value[len(value)-1] == '"' {
							value = value[1 : len(value)-1]
						} else {
							malformed = true
						}
					}
					record[column] = value
				}
				column++
				start = end + 1
				if mask.newlines&bit != 0 {
					if column == width && !malformed {
						visit(record)
					}
					column, malformed = 0, false
				}
				separators -= bit
				quotes ^= seenQuotes
				carriedQuotes = 0
			}
			carriedQuotes += bits.OnesCount32(quotes)
			offset += csvBlock
		}
	}
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
