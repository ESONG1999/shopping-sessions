package main

import (
	"hash/maphash"
	"math/bits"
)

const (
	bucketBits  = 7
	bucketCount = 1 << bucketBits
	blockSize   = 2048
	slabBlocks  = 32
	slabRows    = slabBlocks * blockSize
)

// Fixed-size blocks let workers partition rows without slice growth copying
// the entire bucket. The final block in each worker's bucket may be partial.
type blocks[T any] struct {
	rows [][]T
	size int
}

type partition[T any] struct {
	buckets [bucketCount]blocks[T]
	spare   []T
}

func (p *partition[T]) add(bucket uint64, row T) {
	b := &p.buckets[bucket]
	i := len(b.rows) - 1
	if i < 0 || len(b.rows[i]) == blockSize {
		b.rows = append(b.rows, p.takeBlock())
		i++
	}
	b.rows[i] = append(b.rows[i], row)
	b.size++
}

func (p *partition[T]) takeBlock() []T {
	if len(p.spare) == 0 {
		p.spare = make([]T, slabRows)
	}
	block := p.spare[:0:blockSize]
	p.spare = p.spare[blockSize:]
	return block
}

func takeBlocks[T any](workers []partition[T], bucket int) blocks[T] {
	var result blocks[T]
	for i := range workers {
		b := &workers[i].buckets[bucket]
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

// Each entry carries a packed ID or a hash of an external ID and a reference
// into its worker's payload slabs. Reference zero represents an invalid row.
type eventKey struct {
	key     uint64
	timeRef uint64
}

const (
	extendedEvent = uint32(1) << 31
	rawUserRef    = uint32(1) << 31
	ingestBias    = int64(1) << 30
)

type eventStore struct {
	partition[event]
	slabs [][]event
	next  [bucketCount]uint32
}

func (s *eventStore) append(person uint64, a activity) uint32 {
	bucket := person & (bucketCount - 1)
	b := &s.buckets[bucket]
	i := len(b.rows) - 1
	if i < 0 || len(b.rows[i]) == blockSize {
		if len(s.spare) == 0 {
			s.spare = make([]event, slabRows)
			s.slabs = append(s.slabs, s.spare)
		}
		s.next[bucket] = uint32(len(s.slabs)*slabRows - len(s.spare) + 1)
		b.rows = append(b.rows, s.takeBlock())
		i++
	}
	ref := s.next[bucket]
	s.next[bucket]++
	b.rows[i] = append(b.rows[i], event{person, a})
	b.size++
	return ref
}

func (s *eventStore) discard(ref uint32) {
	if ref != 0 {
		ref--
		s.slabs[ref/slabRows][ref%slabRows].person = 0
	}
}

type inputEvent struct {
	key, user uint64
	ingest    int64
	id        string
	activity
}

type eventDetails struct {
	id     string
	ingest int64
}

type eventLane struct {
	keys     partition[eventKey]
	events   eventStore
	rawUsers eventStore
	details  []eventDetails
	aliases  *aliases
}

type eventInput struct {
	lanes []eventLane
	base  int64
	input mappedChunks
}

func (raw *eventInput) close() {
	for i := range raw.lanes {
		raw.lanes[i].details = nil
	}
	raw.input.close()
}

func externalEventHash(id string) uint64 {
	return max(1, maphash.String(identitySeed, id))
}

func (lane *eventLane) add(row inputEvent, base int64) {
	var ref uint32
	if row.user != 0 {
		hash := mix(row.user)
		if lane.aliases == nil {
			ref = rawUserRef | lane.rawUsers.append(hash, row.activity)
		} else {
			ref = lane.events.append(lane.aliases.canonicalHash(hash), row.activity)
		}
	}
	ingest := row.ingest / 1000
	delta := uint64(ingest - base)
	code := uint32(delta)
	if row.id != "" || delta >= uint64(extendedEvent) {
		code = extendedEvent | uint32(len(lane.details))
		lane.details = append(lane.details, eventDetails{row.id, ingest})
	}
	lane.keys.add(row.key&(bucketCount-1), eventKey{row.key, uint64(code)<<32 | uint64(ref)})
}

func (lane *eventLane) discard(ref uint32) {
	if ref&rawUserRef != 0 {
		lane.rawUsers.discard(ref &^ rawUserRef)
	} else {
		lane.events.discard(ref)
	}
}

func (lane *eventLane) eventDetails(code uint32, base int64) eventDetails {
	if code&extendedEvent != 0 {
		return lane.details[code&^extendedEvent]
	}
	return eventDetails{ingest: base + int64(code)}
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

func readEventCandidates(dir string, workers int, w window, ids *identities, cat catalog, a *aliases, ready <-chan struct{}) (eventInput, error) {
	paths, err := csvPaths(dir)
	if err != nil {
		return eventInput{}, err
	}
	mapped, err := mapChunks(paths)
	if err != nil {
		return eventInput{}, err
	}
	raw := eventInput{lanes: make([]eventLane, workers), base: w.start/1000 - ingestBias, input: mapped}
	decoders := make([]timestampDecoder, workers)
	mapped.scan(workers, func(worker int, text string) {
		lane := &raw.lanes[worker]
		if lane.aliases == nil {
			select {
			case <-ready:
				lane.aliases = a
			default:
			}
		}
		decoder := &decoders[worker]
		if scanNativeEvents(text, w, ids, cat, func(row inputEvent) {
			lane.add(row, raw.base)
		}) {
			return
		}
		scanCSV(text, 7, func(f []string) {
			if row := parseEvent(decoder, f, w, ids, cat); row.key != 0 {
				lane.add(row, raw.base)
			}
		})
	})
	return raw, nil
}

func parseEvent(decoder *timestampDecoder, f []string, w window, ids *identities, cat catalog) inputEvent {
	if f[0] == "" {
		return inputEvent{}
	}
	ingest, ok := decoder.calendar(f[3])
	if !ok {
		return inputEvent{}
	}
	row := inputEvent{key: mix(packIdentity(f[0])), ingest: ingest}
	if row.key == 0 {
		row.key = externalEventHash(f[0])
		row.id = f[0]
	}
	// A validly structured row with invalid activity still takes
	// part in deduplication, with no payload reference.
	if f[1] != "" && f[5] != "" {
		ts, tsOK := decoder.timestamp(f[2])
		if tsOK && w.start <= ts && ts < w.end && (ingest <= ts || ingest-ts <= dayMillis) {
			switch f[4] {
			case "view", "cart":
				row.user = ids.key(f[1])
				row.ts = ts
			case "purchase":
				if price, priceOK := asciiNumber(f[6], 10_000_000); priceOK && price > 0 {
					category, found := cat.products[f[5]]
					if !found {
						category = cat.unknown
					}
					row.user = ids.key(f[1])
					row.activity = activity{ts: ts, cents: uint32(price), category: category}
				}
			}
		}
	}
	return row
}

func deduplicate(raw *eventInput, workers int, a aliases) []partition[event] {
	scratch := make([]dedupScratch, workers)
	parallel(workers, bucketCount, func(worker, bucket int) {
		scratch[worker].dedup(raw, bucket)
	})
	out := make([]partition[event], len(raw.lanes))
	parallel(workers, len(out), func(_, i int) {
		lane := &raw.lanes[i]
		// Events parsed before the alias table was ready are already
		// bucketed by original user. Only a bucket change needs a copy.
		for bucket, b := range lane.rawUsers.buckets {
			for _, block := range b.rows {
				for j := range block {
					row := &block[j]
					if row.person == 0 {
						continue
					}
					person := a.canonicalHash(row.person)
					if person&(bucketCount-1) == uint64(bucket) {
						row.person = person
					} else {
						lane.events.append(person, row.activity)
						row.person = 0
					}
				}
			}
		}
		for bucket, b := range lane.rawUsers.buckets {
			dst := &lane.events.buckets[bucket]
			dst.rows = append(dst.rows, b.rows...)
			dst.size += b.size
		}
		out[i] = lane.events.partition
	})
	return out
}

type eventKeyBlock struct {
	rows []eventKey
	lane *eventLane
}

type dedupScratch struct {
	blocks []eventKeyBlock
	refs   []uint32
	slots  []uint64
}

func clearWords(values []uint64, size int) []uint64 {
	if cap(values) < size {
		return make([]uint64, size)
	}
	values = values[:size]
	clear(values)
	return values
}

func (s *dedupScratch) dedup(raw *eventInput, bucket int) {
	s.blocks = s.blocks[:0]
	size := 0
	for i := range raw.lanes {
		lane := &raw.lanes[i]
		b := &lane.keys.buckets[bucket]
		for _, rows := range b.rows {
			s.blocks = append(s.blocks, eventKeyBlock{rows, lane})
		}
		size += b.size
		*b = blocks[eventKey]{}
	}
	if size == 0 {
		return
	}
	capacity := 1 << bits.Len(uint(4*size-1))
	s.slots = clearWords(s.slots, capacity)
	if cap(s.refs) < capacity {
		s.refs = make([]uint32, capacity)
	}
	refs := s.refs[:capacity]
	mask := capacity - 1
	for blockNo, block := range s.blocks {
		for i, row := range block.rows {
			ref := uint32(blockNo*blockSize + i)
			slot := int(row.key>>bucketBits) & mask
			for {
				previous := s.slots[slot]
				if previous == 0 {
					s.slots[slot] = row.key
					refs[slot] = ref
					break
				}
				if previous == row.key {
					old := refs[slot]
					oldBlock := s.blocks[old/blockSize]
					earlier := oldBlock.rows[old%blockSize]
					t, last := uint32(row.timeRef>>32), uint32(earlier.timeRef>>32)
					replace := t < last
					if (t|last)&extendedEvent != 0 {
						current := block.lane.eventDetails(t, raw.base)
						prior := oldBlock.lane.eventDetails(last, raw.base)
						if current.id != prior.id {
							slot = (slot + 1) & mask
							continue
						}
						replace = current.ingest < prior.ingest
					}
					if replace {
						refs[slot] = ref
						oldBlock.lane.discard(uint32(earlier.timeRef))
					} else {
						block.lane.discard(uint32(row.timeRef))
					}
					break
				}
				slot = (slot + 1) & mask
			}
		}
	}
	clear(s.blocks)
}
