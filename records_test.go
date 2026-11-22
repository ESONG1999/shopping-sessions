package main

import "testing"

func TestEventReferencesAcrossSlabs(t *testing.T) {
	var store eventStore
	const n = slabRows + 831
	refs := make([]uint32, n)
	for i := range refs {
		person := uint64(i%bucketCount + 1)
		refs[i] = store.append(person, activity{ts: int64(i)})
	}
	for i, ref := range refs {
		row := &store.slabs[(ref-1)/slabRows][(ref-1)%slabRows]
		if row.ts != int64(i) || row.person != uint64(i%bucketCount+1) {
			t.Fatalf("reference %d resolved to the wrong row: %+v", ref, *row)
		}
		store.discard(ref)
		if row.person != 0 {
			t.Fatalf("reference %d was not discarded", ref)
		}
	}
}

func TestDedupKeepsDistinctIDsWithTheSameHashSlot(t *testing.T) {
	a := aliases{}
	input := eventInput{lanes: make([]eventLane, 2), base: -ingestBias}
	input.lanes[0].aliases = &a
	add := func(lane int, key, person uint64, ingest, ts int64) {
		input.lanes[lane].add(inputEvent{key: key, user: person, ingest: ingest, activity: activity{ts: ts}}, input.base)
	}
	// The keys start in the same slot. A far-earlier, invalid duplicate
	// suppresses its valid copy, even when it came from another worker.
	const one = uint64(1)
	const two = one | 1<<40
	const three = one | 2<<40
	add(0, one, 1, 5000, 1)
	add(0, two, 2, 1000, 2)
	add(1, one, 0, -62_167_219_200_000, 0)
	add(0, three, 3, 253_402_300_799_000, 11)
	add(1, three, 3, 1000, 3)
	out := deduplicate(&input, 2, a)
	got := make(map[uint64]int64)
	for _, p := range out {
		for _, bucket := range p.buckets {
			for _, block := range bucket.rows {
				for _, row := range block {
					if row.person == 0 {
						continue
					}
					person := unmix(row.person)
					if _, exists := got[person]; exists {
						t.Fatalf("duplicate emitted for person %d", person)
					}
					got[person] = row.ts
				}
			}
		}
	}
	if len(got) != 2 || got[2] != 2 || got[3] != 3 {
		t.Fatalf("wrong survivors: %v", got)
	}
}

func TestEarlyEventMovesToCanonicalBucket(t *testing.T) {
	const user = 1
	original, canonical := mix(user), mix(2)
	if original&(bucketCount-1) == canonical&(bucketCount-1) {
		t.Fatal("test IDs must start in different buckets")
	}
	a := aliases{entries: make(aliasTable, 2), filter: []uint64{aliasBits(original)}}
	a.entries[a.entries.slot(original, 0)] = aliasEntry{hash: original, person: canonical}
	raw := eventInput{lanes: make([]eventLane, 1), base: -ingestBias}
	raw.lanes[0].add(inputEvent{key: 1, user: user, ingest: 1000, activity: activity{ts: 25}}, raw.base)
	out := deduplicate(&raw, 1, a)
	live := 0
	for bucket, b := range out[0].buckets {
		for _, block := range b.rows {
			for _, row := range block {
				if row.person != 0 {
					live++
					if row.person != canonical || row.ts != 25 || uint64(bucket) != canonical&(bucketCount-1) {
						t.Fatalf("wrong row in bucket %d: %+v", bucket, row)
					}
				}
			}
		}
	}
	if live != 1 {
		t.Fatalf("live rows = %d, want 1", live)
	}
}

func TestDedupChecksExternalIDsOnHashCollision(t *testing.T) {
	const hash = 1
	raw := eventInput{lanes: make([]eventLane, 2), base: -ingestBias}
	for _, row := range []inputEvent{
		{key: hash, ingest: 4000, user: 1, activity: activity{ts: 1}},
		{key: hash, id: "extended-event-A", ingest: 4000, user: 2, activity: activity{ts: 2}},
		{key: hash, id: "extended-event-B", ingest: 5000, user: 3, activity: activity{ts: 3}},
	} {
		raw.lanes[0].add(row, raw.base)
	}
	// A malformed payload wins by ingest time and removes only its duplicate.
	raw.lanes[1].add(inputEvent{key: hash, id: "extended-event-A", ingest: 1000}, raw.base)
	// The same external key also collides with the packed ID.
	raw.lanes[1].add(inputEvent{key: hash, id: "extended-event-B", ingest: 1000, user: 4, activity: activity{ts: 4}}, raw.base)
	out := deduplicate(&raw, 2, aliases{})
	survivors := make(map[uint64]bool)
	for _, p := range out {
		for _, b := range p.buckets {
			for _, block := range b.rows {
				for _, row := range block {
					if row.person != 0 {
						survivors[unmix(row.person)] = true
					}
				}
			}
		}
	}
	if len(survivors) != 2 || !survivors[1] || !survivors[4] {
		t.Fatalf("wrong survivors after hash collision: %v", survivors)
	}
}
