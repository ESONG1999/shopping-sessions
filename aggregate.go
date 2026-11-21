package main

import (
	"cmp"
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

type activeCampaign struct {
	last, firstSession, position int
}

type attribution struct {
	sums   map[campaignDay]campaignTotals
	states []activeCampaign
	active []uint32
	clicks []click

	day             int32
	lower, upper    int
	sessionsThisDay int
}

func (a *attribution) record(campaign uint32, delta campaignTotals) {
	key := campaignDay{campaign, a.day}
	total := a.sums[key]
	total.credited += delta.credited
	total.purchasing += delta.purchasing
	total.revenue += delta.revenue
	total.assisted += delta.assisted
	a.sums[key] = total
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
	userRows *tableWriter[userDaily]
	ids      *identities
	names    []string
	revenue  []int64
	touched  []uint32
	ads      attribution
}

func (agg *aggregator) process(events []event, clicks []click) error {
	if len(events) == 0 {
		return nil
	}
	slices.SortFunc(events, func(a, b event) int {
		if a.person != b.person {
			return cmp.Compare(a.person, b.person)
		}
		return cmp.Compare(a.ts, b.ts)
	})
	slices.SortFunc(clicks, func(a, b click) int {
		if a.person != b.person {
			return cmp.Compare(a.person, b.person)
		}
		if a.ts != b.ts {
			return cmp.Compare(a.ts, b.ts)
		}
		// Put the smallest ID last so the last qualifying click wins.
		if agg.ids.less(b.id, a.id) {
			return -1
		}
		if agg.ids.less(a.id, b.id) {
			return 1
		}
		return 0
	})
	clicks = slices.CompactFunc(clicks, func(a, b click) bool { return a.id == b.id })

	clickAt := 0
	for start := 0; start < len(events); {
		person := events[start].person
		end := start + 1
		for end < len(events) && events[end].person == person {
			end++
		}
		for clickAt < len(clicks) && clicks[clickAt].person < person {
			clickAt++
		}
		clickEnd := clickAt
		for clickEnd < len(clicks) && clicks[clickEnd].person == person {
			clickEnd++
		}
		if err := agg.person(events[start:end], clicks[clickAt:clickEnd]); err != nil {
			return err
		}
		start, clickAt = end, clickEnd
	}
	return nil
}

func (agg *aggregator) person(events []event, clicks []click) error {
	name := agg.ids.name(events[0].person)
	day := int32(floorDiv(events[0].ts, dayMillis))
	row := userDaily{UserID: name, Day: day}
	agg.ads.day = day
	agg.ads.clicks = clicks
	for start := 0; start < len(events); {
		ts := events[start].ts
		day = int32(floorDiv(ts, dayMillis))
		if day != row.Day {
			if err := agg.writeDay(row); err != nil {
				return err
			}
			agg.ads.finishDay()
			agg.ads.day = day
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
