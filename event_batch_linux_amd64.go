//go:build nativecsv && cgo

package main

import (
	"shopping-sessions/internal/native"

	"golang.org/x/sys/cpu"
)

func scanNativeEvents(text string, decoder *timestampDecoder, w window, ids *identities, cat catalog, visit func(inputEvent)) bool {
	// A chunk may include a record larger than the nominal chunk size.
	if len(text) > 1<<30 || !cpu.X86.HasAVX2 || !cpu.X86.HasPOPCNT || !cpu.X86.HasBMI1 {
		return false
	}
	var batch [256]native.Candidate
	var f [7]string
	for len(text) != 0 {
		used, n := native.Batch(text, batch[:], w.start, w.end)
		for _, result := range batch[:n] {
			key := result.Key
			if key == 0 {
				start := int(result.Offset)
				if fields(text[start:start+int(result.Meta)], f[:]) {
					if row := parseEvent(decoder, f[:], w, ids, cat); row.key != 0 {
						visit(row)
					}
				}
				continue
			}
			row := inputEvent{
				key: key, user: result.User, ingest: result.Ingest,
				activity: activity{ts: result.TS},
			}
			if meta := result.Meta; meta != 0 {
				start := int(result.Offset)
				category, found := cat.products[text[start:start+int(meta>>24)]]
				if !found {
					category = cat.unknown
				}
				row.cents = meta & 0xffffff
				row.category = category
			}
			visit(row)
		}
		text = text[used:]
	}
	return true
}
