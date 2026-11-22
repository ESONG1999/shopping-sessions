//go:build nativecsv && linux && amd64 && cgo

package native

/*
#cgo CXXFLAGS: -std=c++20 -O3 -mavx2 -mpopcnt -mbmi
#cgo noescape event_batch
#cgo nocallback event_batch
#include "event_batch.h"
*/
import "C"

import "unsafe"

// Candidate mirrors event_candidate. Key zero refers to a raw row stored
// as offset and length. Otherwise Meta holds the 24-bit price and 8-bit
// product length, and Offset points to the product field.
type Candidate struct {
	Key, User    uint64
	Ingest, TS   int64
	Offset, Meta uint32
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
