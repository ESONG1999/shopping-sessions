package main

const wideActivity = uint64(1) << 63

// The timestamp delta sorts before the category and purchase amount. A frame
// too wide for any field uses a lane-local entry with the original values.
type activityCodes struct {
	start int64
	wide  [][]activity
}

func (lane *eventLane) packActivity(row activity) uint64 {
	delta := uint64(row.ts) - uint64(lane.activityStart)
	if delta < 1<<31 && row.category < 1<<8 && row.cents < 1<<24 {
		return delta<<32 | uint64(row.category)<<24 | uint64(row.cents)
	}
	ref := wideActivity | uint64(lane.worker)<<32 | uint64(len(lane.wideActivities))
	lane.wideActivities = append(lane.wideActivities, row)
	return ref
}

func (raw *eventInput) activityCodes(start int64) activityCodes {
	codes := activityCodes{start: start}
	for i := range raw.lanes {
		if rows := raw.lanes[i].wideActivities; len(rows) != 0 {
			if codes.wide == nil {
				codes.wide = make([][]activity, len(raw.lanes))
			}
			codes.wide[i] = rows
		}
	}
	return codes
}

func (codes activityCodes) timestamp(payload uint64) int64 {
	if payload&wideActivity != 0 {
		return codes.wide[(payload&^wideActivity)>>32][uint32(payload)].ts
	}
	return codes.start + int64(payload>>32)
}

func (codes activityCodes) unpack(payload uint64) activity {
	if payload&wideActivity != 0 {
		return codes.wide[(payload&^wideActivity)>>32][uint32(payload)]
	}
	return activity{
		ts:       codes.start + int64(payload>>32),
		cents:    uint32(payload) & (1<<24 - 1),
		category: uint32(payload) >> 24,
	}
}
