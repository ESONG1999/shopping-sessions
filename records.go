package main

import (
	"math/bits"
)

const (
	bucketBits  = 7
	bucketCount = 1 << bucketBits
	blockSize   = 2048
)

// Fixed-size blocks let workers partition rows without slice growth copying
// the entire bucket. The final block in each worker's bucket may be partial.
type blocks[T any] struct {
	rows [][]T
	size int
}

func (b *blocks[T]) add(row T) {
	i := len(b.rows) - 1
	if i < 0 || len(b.rows[i]) == blockSize {
		b.rows = append(b.rows, make([]T, 0, blockSize))
		i++
	}
	b.rows[i] = append(b.rows[i], row)
	b.size++
}

type partition[T any] [bucketCount]blocks[T]

func takeBlocks[T any](workers []partition[T], bucket int) blocks[T] {
	var result blocks[T]
	for i := range workers {
		b := &workers[i][bucket]
		result.rows = append(result.rows, b.rows...)
		result.size += b.size
		*b = blocks[T]{}
	}
	return result
}

func (b blocks[T]) flatten() []T {
	if len(b.rows) == 1 {
		return b.rows[0]
	}
	result := make([]T, 0, b.size)
	for _, block := range b.rows {
		result = append(result, block...)
	}
	return result
}

func mix(key uint64) uint64 {
	key = (key ^ key>>30) * 0xbf58476d1ce4e5b9
	key = (key ^ key>>27) * 0x94d049bb133111eb
	return key ^ key>>31
}

func bucketFor(key uint64) int {
	return int(mix(key) & (bucketCount - 1))
}

type event struct {
	person   uint64
	ts       int64
	cents    uint32
	category uint32
}

type candidate struct {
	id     uint64
	ingest int64
	event
}

type click struct {
	person   uint64
	ts       int64
	id       uint64
	campaign uint32
}

func readEvents(dir string, workers int, w window, ids *identities, a aliases, cat catalog) ([]partition[event], error) {
	raw := make([]partition[candidate], workers)
	if err := scanParts(dir, workers, func(worker int, line string) {
		var f [7]string
		if !fields(line, f[:]) || f[0] == "" {
			return
		}
		ingest, ok := calendarTimestamp(f[3])
		if !ok {
			return
		}
		id := ids.key(f[0])
		e := event{ts: 0}
		// Keep invalid candidates in the dedup table. The earliest copy can
		// suppress a later valid copy, so a zero person marks an invalid winner.
		if f[1] != "" && f[5] != "" {
			ts, tsOK := timestamp(f[2])
			if tsOK && w.start <= ts && ts < w.end && ingest-ts <= dayMillis {
				switch f[4] {
				case "view", "cart":
					e = event{person: a.person(ids.key(f[1])), ts: ts}
				case "purchase":
					if price, priceOK := asciiNumber(f[6], 10_000_000); priceOK && price > 0 {
						category, found := cat.products[f[5]]
						if !found {
							category = cat.unknown
						}
						e = event{person: a.person(ids.key(f[1])), ts: ts, cents: uint32(price), category: category}
					}
				}
			}
		}
		raw[worker][bucketFor(id)].add(candidate{id, ingest, e})
	}); err != nil {
		return nil, err
	}
	byPerson := make([]partition[event], workers)
	parallel(workers, bucketCount, func(worker, bucket int) {
		dedup(takeBlocks(raw, bucket), &byPerson[worker])
	})
	return byPerson, nil
}

func dedup(input blocks[candidate], output *partition[event]) {
	if input.size == 0 {
		return
	}
	// The open-addressing table holds row references. Event keys are exact,
	// so hash collisions affect placement only.
	capacity := 1 << bits.Len(uint(input.size*3/2))
	slots := make([]uint32, capacity)
	for blockNo, block := range input.rows {
		for i, row := range block {
			ref := uint32(blockNo*blockSize + i + 1)
			slot := int(mix(row.id)>>bucketBits) & (capacity - 1)
			for {
				old := slots[slot]
				if old == 0 {
					slots[slot] = ref
					break
				}
				previous := input.rows[(old-1)/blockSize][(old-1)%blockSize]
				if row.id == previous.id {
					if row.ingest < previous.ingest {
						slots[slot] = ref
					}
					break
				}
				slot = (slot + 1) & (capacity - 1)
			}
		}
	}
	for _, ref := range slots {
		if ref > 0 {
			e := input.rows[(ref-1)/blockSize][(ref-1)%blockSize].event
			if e.person != 0 {
				output[bucketFor(e.person)].add(e)
			}
		}
	}
}

func readClicks(dir string, workers int, w window, ids *identities, a aliases, cat catalog) ([]partition[click], error) {
	result := make([]partition[click], workers)
	err := scanParts(dir, workers, func(worker int, line string) {
		var f [4]string
		if !fields(line, f[:]) || f[0] == "" || f[1] == "" {
			return
		}
		ts, ok := timestamp(f[3])
		campaign := cat.campaignIDs[f[2]]
		if !ok || ts < w.start-clickLookback || ts >= w.end || campaign == 0 {
			return
		}
		person := a.person(ids.key(f[1]))
		result[worker][bucketFor(person)].add(click{person, ts, ids.key(f[0]), campaign})
	})
	return result, err
}
