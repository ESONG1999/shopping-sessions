//go:build nativecsv && cgo

package main

import (
	"unsafe"

	"shopping-sessions/internal/native"

	"golang.org/x/sys/unix"
)

var _ [native.DirectKeySize - unsafe.Sizeof(eventKey{})]struct{}
var _ [unsafe.Sizeof(eventKey{}) - native.DirectKeySize]struct{}
var _ [native.DirectEventSize - unsafe.Sizeof(event{})]struct{}
var _ [unsafe.Sizeof(event{}) - native.DirectEventSize]struct{}
var _ [native.DirectAliasSize - unsafe.Sizeof(aliasEntry{})]struct{}
var _ [unsafe.Sizeof(aliasEntry{}) - native.DirectAliasSize]struct{}
var _ [keyBucketCount - native.DirectKeyBuckets]struct{}
var _ [native.DirectKeyBuckets - keyBucketCount]struct{}
var _ [keyBlockSize - native.DirectKeyRows]struct{}
var _ [native.DirectKeyRows - keyBlockSize]struct{}
var _ [bucketCount - native.DirectPersonBuckets]struct{}
var _ [native.DirectPersonBuckets - bucketCount]struct{}
var _ [blockSize - native.DirectPersonRows]struct{}
var _ [native.DirectPersonRows - blockSize]struct{}

type directEventLane struct {
	state        native.DirectState
	keys, events []byte
	keyOwners    []uint16
	eventOwners  []byte
}

func allocateDirectLanes(mapped mappedChunks, workers int) []directEventLane {
	if !nativeEventDecoder() || len(mapped.mappings) == 0 {
		return nil
	}
	var bytes int64
	for _, data := range mapped.mappings {
		bytes += int64(len(data))
	}
	// Reserve addresses without touching pages. The native decoder falls
	// back to Go for any row that would exceed either arena's capacity.
	estimatedRows := bytes / int64(workers) / 42
	keyBlocks := int(min(estimatedRows/keyBlockSize+2*keyBucketCount, int64(directUserRef/keyBlockSize)-1))
	eventBlocks := int(min(estimatedRows/blockSize+2*bucketCount, int64(directUserRef/blockSize)-1))
	keyBytes := keyBlocks * keyBlockSize * int(native.DirectKeySize)
	eventBytes := eventBlocks * blockSize * int(native.DirectEventSize)
	lanes := make([]directEventLane, workers)
	for i := range lanes {
		keys, err := unix.Mmap(-1, 0, keyBytes, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
		if err == nil {
			var events []byte
			events, err = unix.Mmap(-1, 0, eventBytes, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
			if err == nil {
				lanes[i] = directEventLane{
					keys: keys, events: events,
					keyOwners: make([]uint16, keyBlocks), eventOwners: make([]byte, eventBlocks),
				}
				continue
			}
			unix.Munmap(keys)
		}
		for j := range i {
			unix.Munmap(lanes[j].keys)
			unix.Munmap(lanes[j].events)
		}
		return nil
	}
	return lanes
}

func (direct *directEventLane) scan(text string, w window, ids *identities, cat catalog, lane *eventLane, base int64) bool {
	if len(text) > 1<<30 {
		return false
	}
	var entries aliasTable
	var filter []uint64
	raw := uint8(1)
	if lane.aliases != nil {
		raw = 0
		entries, filter = lane.aliases.entries, lane.aliases.filter
	}
	var batch [256]native.Candidate
	for len(text) != 0 {
		used, n := native.DirectBatch(text, batch[:], w.start, w.end, base, &direct.state,
			direct.keys, direct.events, direct.keyOwners, direct.eventOwners, raw,
			unsafe.Pointer(unsafe.SliceData(entries)), len(entries), filter)
		visitCandidates(text, batch[:n], ids, cat, func(row inputEvent) { lane.add(row, base) })
		text = text[used:]
	}
	return true
}

// Attach used blocks to the Go partitions without copying the records.
func (direct *directEventLane) publish(raw *eventInput, worker int) {
	raw.directKeys = append(raw.directKeys, direct.keys)
	raw.directMemory = append(raw.directMemory, direct.events)
	lane := &raw.lanes[worker]
	keys := unsafe.Slice((*eventKey)(unsafe.Pointer(unsafe.SliceData(direct.keys))), int(direct.state.KeyBlocks)*keyBlockSize)
	events := unsafe.Slice((*event)(unsafe.Pointer(unsafe.SliceData(direct.events))), int(direct.state.EventBlocks)*blockSize)
	lane.directPayload = events
	for block, bucket := range direct.keyOwners[:direct.state.KeyBlocks] {
		start := block * keyBlockSize
		end := min(start+keyBlockSize, int(direct.state.KeyNext[bucket])-1)
		b := &lane.keys.buckets[bucket]
		b.rows = append(b.rows, keys[start:end:start+keyBlockSize])
		b.size += end - start
	}
	for block, bucket := range direct.eventOwners[:direct.state.EventBlocks] {
		start := block * blockSize
		end := min(start+blockSize, int(direct.state.EventNext[bucket])-1)
		store := &lane.events
		if bucket >= bucketCount {
			bucket -= bucketCount
			store = &lane.rawUsers
		}
		b := &store.buckets[bucket]
		b.rows = append(b.rows, events[start:end:start+blockSize])
		b.size += end - start
	}
}
