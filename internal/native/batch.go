//go:build nativecsv && linux && amd64 && cgo

package native

/*
#cgo CXXFLAGS: -std=c++20 -O3 -mavx2 -mpopcnt -mbmi
#cgo noescape event_batch
#cgo nocallback event_batch
#cgo noescape event_batch_direct
#cgo nocallback event_batch_direct
#include "event_batch.h"
*/
import "C"

import "unsafe"

// Candidate mirrors event_candidate. Key and User hold packed IDs or, with
// bit 63 set, the byte length and offset of an ID in the input batch.
type Candidate struct {
	Key, User                  uint64
	Ingest, TS                 int64
	ProductOffset, ProductSize uint32
	Cents                      uint32
}

const candidateSize = unsafe.Sizeof(C.struct_event_candidate{})

var _ [candidateSize - unsafe.Sizeof(Candidate{})]struct{}
var _ [unsafe.Sizeof(Candidate{}) - candidateSize]struct{}

// Batch consumes whole CSV records until output is full or input is exhausted.
func Batch(text string, output []Candidate, start, end int64) (int, int) {
	n := uint64(C.event_batch(
		(*C.char)(unsafe.Pointer(unsafe.StringData(text))), C.uint32_t(len(text)),
		(*C.struct_event_candidate)(unsafe.Pointer(&output[0])), C.uint32_t(len(output)), C.int64_t(start), C.int64_t(end),
	))
	return int(n >> 32), int(uint32(n))
}

const (
	DirectKeyBuckets    = C.direct_key_buckets
	DirectKeyRows       = C.direct_key_rows
	DirectPersonBuckets = C.direct_person_buckets
	DirectPersonRows    = C.direct_person_rows
)

type DirectState struct {
	KeyNext                [DirectKeyBuckets]uint32
	EventNext              [2 * DirectPersonBuckets]uint32
	KeyBlocks, EventBlocks uint32
}

const (
	directStateSize = unsafe.Sizeof(C.struct_direct_state{})
	DirectKeySize   = unsafe.Sizeof(C.struct_direct_key{})
	DirectEventSize = unsafe.Sizeof(C.struct_direct_event{})
	DirectAliasSize = unsafe.Sizeof(C.struct_direct_alias{})
)

var _ [directStateSize - unsafe.Sizeof(DirectState{})]struct{}
var _ [unsafe.Sizeof(DirectState{}) - directStateSize]struct{}

func DirectBatch(text string, output []Candidate, start, end, base int64, state *DirectState,
	keys, events []byte, keyOwners []uint16, eventOwners []byte, raw uint8, aliases unsafe.Pointer, aliasCount int, filter []uint64) (int, int) {
	n := uint64(C.event_batch_direct(
		(*C.char)(unsafe.Pointer(unsafe.StringData(text))), C.uint32_t(len(text)),
		(*C.struct_event_candidate)(unsafe.Pointer(&output[0])), C.uint32_t(len(output)),
		C.int64_t(start), C.int64_t(end), C.int64_t(base),
		(*C.struct_direct_state)(unsafe.Pointer(state)),
		(*C.struct_direct_key)(unsafe.Pointer(unsafe.SliceData(keys))),
		(*C.struct_direct_event)(unsafe.Pointer(unsafe.SliceData(events))),
		(*C.uint16_t)(unsafe.Pointer(unsafe.SliceData(keyOwners))),
		(*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(eventOwners))),
		C.uint32_t(len(keyOwners)), C.uint32_t(len(eventOwners)), C.uint8_t(raw),
		(*C.struct_direct_alias)(aliases), C.uint32_t(aliasCount),
		(*C.uint64_t)(unsafe.Pointer(unsafe.SliceData(filter))), C.uint32_t(len(filter)),
	))
	return int(n >> 32), int(uint32(n))
}
