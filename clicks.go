package main

import "unsafe"

type clickInput struct {
	partitions []partition[click]
	ids        clickIDs
	input      mappedChunks
}

func (in *clickInput) close() {
	in.input.close()
}

// Click IDs borrow their exact, unquoted bytes from the mapped input. A
// reference holds a chunk index, byte length, and offset, in that order.
type clickIDs struct {
	chunks []string
	wide   [][]string
}

func (ids *clickIDs) reference(id string, base uintptr, chunk, worker int) uint64 {
	offset := uintptr(unsafe.Pointer(unsafe.StringData(id))) - base
	if offset <= 1<<32-1 && len(id) <= 1<<16-1 && chunk <= 1<<16-1 {
		return uint64(chunk)<<48 | uint64(len(id))<<32 | uint64(offset)
	}
	// IDs are non-empty. Length zero selects the worker and side-table index.
	index := len(ids.wide[worker])
	ids.wide[worker] = append(ids.wide[worker], id)
	return uint64(worker)<<48 | uint64(uint32(index))
}

func (ids *clickIDs) name(ref uint64) string {
	chunk, length, offset := int(ref>>48), int(uint16(ref>>32)), int(uint32(ref))
	if length == 0 {
		return ids.wide[chunk][offset]
	}
	return ids.chunks[chunk][offset : offset+length]
}

func readClicks(dir string, workers int, w window, identities *identities, a aliases, cat catalog) (clickInput, error) {
	paths, err := csvPaths(dir)
	if err != nil {
		return clickInput{}, err
	}
	mapped, err := mapChunks(paths)
	if err != nil {
		return clickInput{}, err
	}
	result := clickInput{
		partitions: make([]partition[click], workers),
		ids:        clickIDs{chunks: make([]string, len(mapped.jobs)), wide: make([][]string, workers)},
		input:      mapped,
	}
	decoders := make([]timestampDecoder, workers)
	parallel(workers, len(mapped.jobs), func(worker, job int) {
		text := mapped.jobs[job].records()
		result.ids.chunks[job] = text
		base := uintptr(unsafe.Pointer(unsafe.StringData(text)))
		decoder := &decoders[worker]
		rows := &result.partitions[worker]
		scanCSV(text, 4, func(f []string) {
			if f[0] == "" || f[1] == "" {
				return
			}
			ts, ok := decoder.timestamp(f[3])
			campaign := cat.campaignIDs[f[2]]
			if !ok || ts < w.start-clickLookback || ts >= w.end || campaign == 0 {
				return
			}
			person := a.personHash(identities.key(f[1]))
			id := result.ids.reference(f[0], base, job, worker)
			rows.add(person&(bucketCount-1), click{person, touchpoint{ts, id, campaign}})
		})
	})
	return result, nil
}
