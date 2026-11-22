//go:build nativecsv && cgo

package main

import (
	"shopping-sessions/internal/native"

	"golang.org/x/sys/cpu"
)

func scanNativeEvents(text string, w window, ids *identities, cat catalog, visit func(inputEvent)) bool {
	// A chunk may include a record larger than the nominal chunk size.
	if len(text) > 1<<30 || !cpu.X86.HasAVX2 || !cpu.X86.HasPOPCNT || !cpu.X86.HasBMI1 {
		return false
	}
	var batch [256]native.Candidate
	for len(text) != 0 {
		used, n := native.Batch(text, batch[:], w.start, w.end)
		for _, result := range batch[:n] {
			row := inputEvent{
				user: result.User, ingest: result.Ingest,
				activity: activity{ts: result.TS, cents: result.Cents},
			}
			if result.Key&externalID != 0 {
				row.id = batchID(text, result.Key)
				row.key = externalEventHash(row.id)
			} else {
				row.key = mix(result.Key)
			}
			if row.user&externalID != 0 {
				row.user = ids.intern(batchID(text, row.user))
			}
			if row.cents != 0 {
				start := int(result.ProductOffset)
				category, found := cat.products[text[start:start+int(result.ProductSize)]]
				if !found {
					category = cat.unknown
				}
				row.category = category
			}
			visit(row)
		}
		text = text[used:]
	}
	return true
}

func batchID(text string, id uint64) string {
	start := int(uint32(id))
	return text[start : start+int((id&^externalID)>>32)]
}
