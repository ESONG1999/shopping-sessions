package main

import (
	"cmp"
	"math/bits"
	"slices"
)

type userDaily struct {
	UserID       string  `parquet:"user_id,dict"`
	Day          int32   `parquet:"day,date"`
	Sessions     int64   `parquet:"sessions"`
	Events       int64   `parquet:"events"`
	RevenueCents int64   `parquet:"revenue_cents"`
	TopCategory  *string `parquet:"top_category,optional,dict"`
}

type campaignDaily struct {
	CampaignID         string `parquet:"campaign_id,dict"`
	Channel            string `parquet:"channel,dict"`
	Day                int32  `parquet:"day,date"`
	Sessions           int64  `parquet:"sessions"`
	PurchasingSessions int64  `parquet:"purchasing_sessions"`
	RevenueCents       int64  `parquet:"revenue_cents"`
	AssistedSessions   int64  `parquet:"assisted_sessions"`
}

type campaignDay struct {
	campaign uint32
	day      int32
}

type campaignTotals struct {
	credited, purchasing, revenue, assisted int64
}

func (total *campaignTotals) add(delta campaignTotals) {
	total.credited += delta.credited
	total.purchasing += delta.purchasing
	total.revenue += delta.revenue
	total.assisted += delta.assisted
}

type activeCampaign struct {
	last, firstSession, position int
}

type attribution struct {
	sums   map[campaignDay]campaignTotals
	dense  []campaignTotals
	states []activeCampaign
	active []uint32
	clicks []touchpoint

	firstDay, day   int32
	dayOffset       int
	lower, upper    int
	sessionsThisDay int
}

func (a *attribution) record(campaign uint32, delta campaignTotals) {
	if a.dense != nil {
		a.dense[a.dayOffset+int(campaign)].add(delta)
		return
	}
	key := campaignDay{campaign, a.day}
	total := a.sums[key]
	total.add(delta)
	a.sums[key] = total
}

func (a *attribution) setDay(day int32) {
	a.day = day
	a.dayOffset = int(int64(day)-int64(a.firstDay)) * len(a.states)
}

func (a *attribution) countAssists(campaign uint32) {
	delta := a.sessionsThisDay - a.states[campaign].firstSession
	if delta > 0 {
		a.record(campaign, campaignTotals{assisted: int64(delta)})
	}
}

func (a *attribution) finishDay() {
	for _, campaign := range a.active {
		a.countAssists(campaign)
		a.states[campaign].firstSession = 0
	}
	a.sessionsThisDay = 0
}

func (a *attribution) finishPerson() {
	a.finishDay()
	for _, campaign := range a.active {
		a.states[campaign] = activeCampaign{}
	}
	a.active = a.active[:0]
	a.lower, a.upper = 0, 0
	a.clicks = nil
}

// The active set keeps the last click per campaign. Count a whole span of
// assisted sessions on expiry or at a day boundary.
func (a *attribution) session(start, revenue int64) {
	for a.upper < len(a.clicks) && a.clicks[a.upper].ts <= start {
		campaign := a.clicks[a.upper].campaign
		a.upper++
		state := &a.states[campaign]
		if state.last == 0 {
			*state = activeCampaign{firstSession: a.sessionsThisDay, position: len(a.active)}
			a.active = append(a.active, campaign)
		}
		state.last = a.upper
	}
	for a.lower < a.upper && a.clicks[a.lower].ts < start-clickLookback {
		campaign := a.clicks[a.lower].campaign
		a.lower++
		state := &a.states[campaign]
		if state.last == a.lower {
			a.countAssists(campaign)
			last := a.active[len(a.active)-1]
			a.active[state.position] = last
			a.states[last].position = state.position
			a.active = a.active[:len(a.active)-1]
			*state = activeCampaign{}
		}
	}
	var credited uint32
	if a.upper > a.lower {
		credited = a.clicks[a.upper-1].campaign
	}
	purchasing := int64(0)
	if revenue > 0 {
		purchasing = 1
	}
	a.record(credited, campaignTotals{credited: 1, purchasing: purchasing, revenue: revenue})
	a.sessionsThisDay++
}

type aggregator struct {
	userRows    *tableWriter[userDaily]
	ids         *identities
	names       []string
	revenue     []int64
	touched     []uint32
	timeScratch []activity
	personIndex aliasTable
	ads         attribution
}

type personSpan struct {
	id             uint64
	events, clicks int
}

func (agg *aggregator) process(events blocks[event], clicks blocks[click]) error {
	if events.size == 0 {
		return nil
	}
	// Count both inputs by person. After the prefix sums, the same fields
	// act as write cursors into compact, contiguous spans.
	spans := []personSpan{{}}
	size := 1 << bits.Len(uint(max(31, events.size/16)))
	if cap(agg.personIndex) < size {
		agg.personIndex = make(aliasTable, size)
	} else {
		agg.personIndex = agg.personIndex[:size]
		clear(agg.personIndex)
	}
	index := agg.personIndex
	for _, block := range events.rows {
		for j := range block {
			row := &block[j]
			hash := row.person
			slot := index.slot(hash, bucketBits)
			i := index[slot].person
			if i == 0 {
				if (len(spans)-1)*2 == len(index) {
					index = index.grow(bucketBits)
					slot = index.slot(hash, bucketBits)
				}
				i = uint64(len(spans))
				index[slot] = aliasEntry{hash, i}
				spans = append(spans, personSpan{id: hash})
			}
			spans[i].events++
			// The input block is owned by this job. Reuse its key column
			// for the dense index needed by the scatter pass.
			row.person = i
		}
	}
	for _, block := range clicks.rows {
		for j := range block {
			row := &block[j]
			i := index[index.slot(row.person, bucketBits)].person
			spans[i].clicks++
			row.person = i
		}
	}
	agg.personIndex = index
	eventCount, clickCount := 0, 0
	for i := 1; i < len(spans); i++ {
		span := &spans[i]
		span.events, eventCount = eventCount, eventCount+span.events
		span.clicks, clickCount = clickCount, clickCount+span.clicks
	}
	eventRows := make([]activity, eventCount)
	clickRows := make([]touchpoint, clickCount)
	for _, block := range events.rows {
		for _, row := range block {
			span := &spans[row.person]
			eventRows[span.events] = row.activity
			span.events++
		}
	}
	for _, block := range clicks.rows {
		for _, row := range block {
			if i := row.person; i != 0 {
				span := &spans[i]
				clickRows[span.clicks] = row.touchpoint
				span.clicks++
			}
		}
	}

	eventAt, clickAt := 0, 0
	for _, span := range spans[1:] {
		personEvents := agg.sortActivity(eventRows[eventAt:span.events])
		personClicks := clickRows[clickAt:span.clicks]
		if len(personClicks) > 1 {
			slices.SortFunc(personClicks, agg.compareClicks)
			personClicks = slices.CompactFunc(personClicks, func(a, b touchpoint) bool { return a.id == b.id })
		}
		if err := agg.person(unmix(span.id), personEvents, personClicks); err != nil {
			return err
		}
		eventAt, clickAt = span.events, span.clicks
	}
	return nil
}

func (agg *aggregator) compareClicks(a, b touchpoint) int {
	if a.ts != b.ts {
		return cmp.Compare(a.ts, b.ts)
	}
	// Put the smallest ID last so the last qualifying click wins.
	if (a.id|b.id)&externalID == 0 {
		return cmp.Compare(b.id, a.id)
	}
	return cmp.Compare(agg.ids.name(b.id), agg.ids.name(a.id))
}

func (agg *aggregator) person(id uint64, events []activity, clicks []touchpoint) error {
	name := agg.ids.name(id)
	day := int32(floorDiv(events[0].ts, dayMillis))
	row := userDaily{UserID: name, Day: day}
	agg.ads.setDay(day)
	agg.ads.clicks = clicks
	for start := 0; start < len(events); {
		ts := events[start].ts
		day = int32(floorDiv(ts, dayMillis))
		if day != row.Day {
			if err := agg.writeDay(row); err != nil {
				return err
			}
			agg.ads.finishDay()
			agg.ads.setDay(day)
			row = userDaily{UserID: name, Day: day}
		}

		end := start
		var revenue int64
		for {
			e := events[end]
			if e.cents != 0 {
				if agg.revenue[e.category] == 0 {
					agg.touched = append(agg.touched, e.category)
				}
				agg.revenue[e.category] += int64(e.cents)
				revenue += int64(e.cents)
			}
			end++
			if end == len(events) || events[end].ts-events[end-1].ts > sessionGap {
				break
			}
		}
		row.Sessions++
		row.Events += int64(end - start)
		row.RevenueCents += revenue
		agg.ads.session(ts, revenue)
		start = end
	}
	agg.ads.finishPerson()
	return agg.writeDay(row)
}

func (agg *aggregator) writeDay(row userDaily) error {
	var best uint32
	var bestRevenue int64
	for _, category := range agg.touched {
		revenue := agg.revenue[category]
		if revenue > bestRevenue || (revenue == bestRevenue && category < best) {
			best, bestRevenue = category, revenue
		}
		agg.revenue[category] = 0
	}
	agg.touched = agg.touched[:0]
	if bestRevenue > 0 {
		row.TopCategory = &agg.names[best]
	}
	return agg.userRows.add(row)
}
