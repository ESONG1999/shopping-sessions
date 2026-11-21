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

func mix(key uint64) uint64 {
	key = (key ^ key>>30) * 0xbf58476d1ce4e5b9
	key = (key ^ key>>27) * 0x94d049bb133111eb
	return key ^ key>>31
}

func bucketFor(key uint64) int {
	return int(mix(key) & (bucketCount - 1))
}

type event struct {
	person uint64
	activity
}

type activity struct {
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
	person uint64
	touchpoint
}

type touchpoint struct {
	ts       int64
	id       uint64
	campaign uint32
}

func readEventCandidates(dir string, workers int, w window, ids *identities, cat catalog) ([]partition[candidate], error) {
	raw := make([]partition[candidate], workers)
	if err := scanParts(dir, workers, 7, func(worker int, f []string) {
		if f[0] == "" {
			return
		}
		ingest, ok := calendarTimestamp(f[3])
		if !ok {
			return
		}
		id := ids.key(f[0])
		var e event
		// Keep invalid candidates in the dedup table. The earliest copy can
		// suppress a later valid copy, so a zero person marks an invalid winner.
		if f[1] != "" && f[5] != "" {
			ts, tsOK := timestamp(f[2])
			if tsOK && w.start <= ts && ts < w.end && ingest-ts <= dayMillis {
				switch f[4] {
				case "view", "cart":
					e = event{person: ids.key(f[1]), activity: activity{ts: ts}}
				case "purchase":
					if price, priceOK := asciiNumber(f[6], 10_000_000); priceOK && price > 0 {
						category, found := cat.products[f[5]]
						if !found {
							category = cat.unknown
						}
						e = event{
							person:   ids.key(f[1]),
							activity: activity{ts: ts, cents: uint32(price), category: category},
						}
					}
				}
			}
		}
		raw[worker][bucketFor(id)].add(candidate{id, ingest, e})
	}); err != nil {
		return nil, err
	}
	return raw, nil
}

func deduplicate(raw []partition[candidate], workers int, a aliases) []partition[event] {
	byPerson := make([]partition[event], workers)
	parallel(workers, bucketCount, func(worker, bucket int) {
		dedup(takeBlocks(raw, bucket), a, &byPerson[worker])
	})
	return byPerson
}

func dedup(input blocks[candidate], a aliases, output *partition[event]) {
	if input.size == 0 {
		return
	}
	// A slot holds a fingerprint and a row reference. Probe the candidate
	// block only when the fingerprints agree, then verify the complete ID.
	capacity := 1 << bits.Len(uint(input.size*3/2))
	slots := make([]uint64, capacity)
	for blockNo, block := range input.rows {
		for i := range block {
			row := &block[i]
			ref := uint64(blockNo*blockSize + i + 1)
			hash := mix(row.id) >> bucketBits
			fingerprint := hash & 0xffffffff00000000
			slot := int(hash) & (capacity - 1)
			for {
				old := slots[slot]
				if old == 0 {
					slots[slot] = fingerprint | ref
					break
				}
				if old&0xffffffff00000000 == fingerprint {
					index := uint32(old) - 1
					previous := &input.rows[index/blockSize][index%blockSize]
					if row.id == previous.id {
						if row.ingest < previous.ingest {
							slots[slot] = fingerprint | ref
							previous.person = 0
						} else {
							row.person = 0
						}
						break
					}
				}
				slot = (slot + 1) & (capacity - 1)
			}
		}
	}
	// Stream the winners in input order. The hash table is never used to
	// revisit each unique event through an unrelated memory address.
	for _, block := range input.rows {
		for _, row := range block {
			if row.person != 0 {
				e := row.event
				e.person = a.person(e.person)
				output[bucketFor(e.person)].add(e)
			}
		}
	}
}

func readClicks(dir string, workers int, w window, ids *identities, a aliases, cat catalog) ([]partition[click], error) {
	result := make([]partition[click], workers)
	err := scanParts(dir, workers, 4, func(worker int, f []string) {
		if f[0] == "" || f[1] == "" {
			return
		}
		ts, ok := timestamp(f[3])
		campaign := cat.campaignIDs[f[2]]
		if !ok || ts < w.start-clickLookback || ts >= w.end || campaign == 0 {
			return
		}
		person := a.person(ids.key(f[1]))
		result[worker][bucketFor(person)].add(click{person, touchpoint{ts, ids.key(f[0]), campaign}})
	})
	return result, err
}
