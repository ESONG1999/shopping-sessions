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
	// Odd multiplication and right xorshift are each reversible on uint64.
	key = (key ^ key>>30) * 0xbf58476d1ce4e5b9
	key = (key ^ key>>27) * 0x94d049bb133111eb
	return key ^ key>>31
}

func unmix(key uint64) uint64 {
	// Undo mix in reverse order. The constants are the multiplicative
	// inverses modulo 2^64.
	key = (key ^ key>>31 ^ key>>62) * 0x319642b2d24d8ec3
	key = (key ^ key>>27 ^ key>>54) * 0x96de1b173f119089
	return key ^ key>>30 ^ key>>60
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
	key            uint64
	person         uint64
	timeCategory   uint64
	ingestAndPrice uint64
}

const (
	candidatePriceBits = 24
	candidateTimeBits  = 48
	candidateWide      = uint64(1) << 63
	ingestBias         = int64(1) << 38
)

type candidateInput struct {
	byKey []partition[candidate]
	wide  [][]activity
	start int64
}

func (in *candidateInput) add(worker int, key uint64, ingest int64, user uint64, e activity) {
	// The four-digit calendar fits in 39 signed seconds. Biasing the
	// seconds leaves room for the price and an overflow flag.
	row := candidate{
		key:            key,
		person:         user,
		ingestAndPrice: uint64(ingest/1000+ingestBias)<<candidatePriceBits | uint64(e.cents),
	}
	if user != 0 {
		delta := uint64(e.ts) - uint64(in.start)
		if delta>>candidateTimeBits == 0 && e.category>>16 == 0 {
			row.timeCategory = uint64(e.category)<<candidateTimeBits | delta
		} else {
			row.ingestAndPrice |= candidateWide
			row.timeCategory = uint64(worker)<<32 | uint64(len(in.wide[worker]))
			in.wide[worker] = append(in.wide[worker], e)
		}
	}
	in.byKey[worker][key&(bucketCount-1)].add(row)
}

func (row candidate) ingestOrder() uint64 {
	return (row.ingestAndPrice &^ candidateWide) >> candidatePriceBits
}

func (in candidateInput) activity(row candidate) activity {
	if row.ingestAndPrice&candidateWide != 0 {
		return in.wide[row.timeCategory>>32][uint32(row.timeCategory)]
	}
	return activity{
		ts:       in.start + int64(row.timeCategory&((1<<candidateTimeBits)-1)),
		cents:    uint32(row.ingestAndPrice & ((1 << candidatePriceBits) - 1)),
		category: uint32(row.timeCategory >> candidateTimeBits),
	}
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

func readEventCandidates(dir string, workers int, w window, ids *identities, cat catalog) (candidateInput, error) {
	raw := candidateInput{
		byKey: make([]partition[candidate], workers),
		wide:  make([][]activity, workers),
		start: w.start,
	}
	decoders := make([]timestampDecoder, workers)
	if err := scanParts(dir, workers, 7, func(worker int, f []string) {
		if f[0] == "" {
			return
		}
		decoder := &decoders[worker]
		ingest, ok := decoder.calendar(f[3])
		if !ok {
			return
		}
		key := mix(ids.key(f[0]))
		var user uint64
		var e activity
		// Keep invalid candidates in the dedup table. The earliest copy can
		// suppress a later valid copy, so a zero person marks an invalid winner.
		if f[1] != "" && f[5] != "" {
			ts, tsOK := decoder.timestamp(f[2])
			if tsOK && w.start <= ts && ts < w.end && ingest-ts <= dayMillis {
				switch f[4] {
				case "view", "cart":
					user = ids.key(f[1])
					e = activity{ts: ts}
				case "purchase":
					if price, priceOK := asciiNumber(f[6], 10_000_000); priceOK && price > 0 {
						category, found := cat.products[f[5]]
						if !found {
							category = cat.unknown
						}
						user = ids.key(f[1])
						e = activity{ts: ts, cents: uint32(price), category: category}
					}
				}
			}
		}
		raw.add(worker, key, ingest, user, e)
	}); err != nil {
		return candidateInput{}, err
	}
	return raw, nil
}

func deduplicate(raw candidateInput, workers int, a aliases) []partition[event] {
	byPerson := make([]partition[event], workers)
	scratch := make([]dedupScratch, workers)
	parallel(workers, bucketCount, func(worker, bucket int) {
		scratch[worker].dedup(takeBlocks(raw.byKey, bucket), raw, a, &byPerson[worker])
	})
	return byPerson
}

type dedupScratch struct {
	counters []uint64
	slots    []uint64
}

func clearWords(values []uint64, size int) []uint64 {
	if cap(values) < size {
		return make([]uint64, size)
	}
	values = values[:size]
	clear(values)
	return values
}

func (s *dedupScratch) dedup(input blocks[candidate], raw candidateInput, a aliases, output *partition[event]) {
	if input.size == 0 {
		return
	}
	// A two-bit counter records whether each hashed position was seen once
	// or multiple times. A singleton cannot have an equal candidate.
	words := 1 << bits.Len(uint((input.size-1)/2))
	s.counters = clearWords(s.counters, words)
	mask := uint64(words - 1)
	suspects := 0
	for _, block := range input.rows {
		for _, row := range block {
			hash := row.key >> bucketBits
			index := (hash >> 5) & mask
			seen := uint64(1) << ((hash & 31) << 1)
			state := s.counters[index]
			if state&seen == 0 {
				s.counters[index] = state | seen
			} else {
				repeated := seen << 1
				if state&repeated == 0 {
					s.counters[index] = state | repeated
					suspects++
				}
				suspects++
			}
		}
	}

	// A collision in the counters goes through exact deduplication. Use
	// different hash bits for placement so counter collisions spread out.
	capacity := 1 << bits.Len(uint(suspects*3/2))
	s.slots = clearWords(s.slots, capacity)
	for blockNo, block := range input.rows {
		for i, row := range block {
			hash := row.key >> bucketBits
			index := (hash >> 5) & mask
			repeated := uint64(2) << ((hash & 31) << 1)
			if s.counters[index]&repeated == 0 {
				emitWinner(row, raw, a, output)
				continue
			}
			ref := uint64(blockNo*blockSize + i + 1)
			fingerprint := row.key << 32
			slot := int(row.key>>32) & (capacity - 1)
			for {
				old := s.slots[slot]
				if old == 0 {
					s.slots[slot] = fingerprint | ref
					break
				}
				if old&0xffffffff00000000 == fingerprint {
					index := uint32(old) - 1
					previous := input.rows[index/blockSize][index%blockSize]
					if row.key == previous.key {
						if row.ingestOrder() < previous.ingestOrder() {
							s.slots[slot] = fingerprint | ref
						}
						break
					}
				}
				slot = (slot + 1) & (capacity - 1)
			}
		}
	}
	for _, slot := range s.slots {
		if slot != 0 {
			index := uint32(slot) - 1
			emitWinner(input.rows[index/blockSize][index%blockSize], raw, a, output)
		}
	}
}

func emitWinner(row candidate, raw candidateInput, a aliases, output *partition[event]) {
	if row.person != 0 {
		person := a.personHash(row.person)
		output[person&(bucketCount-1)].add(event{person, raw.activity(row)})
	}
}

func readClicks(dir string, workers int, w window, ids *identities, a aliases, cat catalog) ([]partition[click], error) {
	result := make([]partition[click], workers)
	decoders := make([]timestampDecoder, workers)
	err := scanParts(dir, workers, 4, func(worker int, f []string) {
		if f[0] == "" || f[1] == "" {
			return
		}
		ts, ok := decoders[worker].timestamp(f[3])
		campaign := cat.campaignIDs[f[2]]
		if !ok || ts < w.start-clickLookback || ts >= w.end || campaign == 0 {
			return
		}
		person := a.personHash(ids.key(f[1]))
		result[worker][person&(bucketCount-1)].add(click{person, touchpoint{ts, ids.key(f[0]), campaign}})
	})
	return result, err
}
