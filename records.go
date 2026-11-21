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

func (in *candidateInput) add(worker int, key uint64, ingest int64, e event) {
	// The four-digit calendar fits in 39 signed seconds. Biasing the
	// seconds leaves room for the price and an overflow flag.
	row := candidate{
		key:            key,
		person:         e.person,
		ingestAndPrice: uint64(ingest/1000+ingestBias)<<candidatePriceBits | uint64(e.cents),
	}
	if e.person != 0 {
		delta := uint64(e.ts) - uint64(in.start)
		if delta>>candidateTimeBits == 0 && e.category>>16 == 0 {
			row.timeCategory = uint64(e.category)<<candidateTimeBits | delta
		} else {
			row.ingestAndPrice |= candidateWide
			row.timeCategory = uint64(worker)<<32 | uint64(len(in.wide[worker]))
			in.wide[worker] = append(in.wide[worker], e.activity)
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
		var e event
		// Keep invalid candidates in the dedup table. The earliest copy can
		// suppress a later valid copy, so a zero person marks an invalid winner.
		if f[1] != "" && f[5] != "" {
			ts, tsOK := decoder.timestamp(f[2])
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
		raw.add(worker, key, ingest, e)
	}); err != nil {
		return candidateInput{}, err
	}
	return raw, nil
}

func deduplicate(raw candidateInput, workers int, a aliases) []partition[event] {
	byPerson := make([]partition[event], workers)
	parallel(workers, bucketCount, func(worker, bucket int) {
		dedup(takeBlocks(raw.byKey, bucket), raw, a, &byPerson[worker])
	})
	return byPerson
}

func dedup(input blocks[candidate], raw candidateInput, a aliases, output *partition[event]) {
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
			hash := row.key >> bucketBits
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
					if row.key == previous.key {
						if row.ingestOrder() < previous.ingestOrder() {
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
				person, bucket := a.personBucket(row.person)
				output[bucket].add(event{person, raw.activity(row)})
			}
		}
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
		person, bucket := a.personBucket(ids.key(f[1]))
		result[worker][bucket].add(click{person, touchpoint{ts, ids.key(f[0]), campaign}})
	})
	return result, err
}
