package main

import (
	"cmp"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestMappedClickIDsPreserveExactBytes(t *testing.T) {
	dir := t.TempDir()
	long := "é" + strings.Repeat("a", 1<<16)
	body := "click_id,user_id,campaign_id,ts\n\" b\",u,c_1,1800000\n" + long + ",u,c_1,1800000"
	if err := os.WriteFile(filepath.Join(dir, "part.csv"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	ids := &identities{}
	input, err := readClicks(dir, 2, window{start: 0, end: dayMillis}, ids, aliases{}, catalog{campaignIDs: map[string]uint32{"c_1": 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer input.close()
	b := takeBlocks(input.partitions, int(mix(ids.key("u"))&(bucketCount-1)))
	if b.size != 2 {
		t.Fatalf("got %d clicks, want 2", b.size)
	}
	var got []string
	for _, block := range b.rows {
		for _, row := range block {
			got = append(got, input.ids.name(row.id))
		}
	}
	if !slices.Equal(got, []string{" b", long}) {
		t.Fatalf("decoded click IDs have wrong bytes or lengths")
	}
}

func TestSingleAndRollingAttributionChooseExactClickID(t *testing.T) {
	names := clickIDs{chunks: []string{"z", " a-long-click-id", "expired", "boundary", "future"}}
	point := func(id int, ts int64, campaign uint32) touchpoint {
		return touchpoint{ts: ts, id: uint64(id)<<48 | uint64(len(names.chunks[id]))<<32, campaign: campaign}
	}
	start := int64(clickLookback)
	clicks := []touchpoint{
		point(0, start, 1),
		point(3, 0, 3),
		point(1, start, 2),
		point(1, start, 2),
		point(2, -1, 4),
		point(4, start+1, 4),
	}
	for _, single := range []bool{true, false} {
		rows := slices.Clone(clicks)
		a := attribution{clicks: rows, clickIDs: &names, states: make([]activeCampaign, 5), sums: make(map[campaignDay]campaignTotals)}
		a.setDay(7)
		if single {
			a.singleSession(start, 129)
		} else {
			slices.SortFunc(rows, func(a, b touchpoint) int { return cmp.Compare(a.ts, b.ts) })
			a.session(start, 129)
		}
		a.finishPerson()
		wantWinner := campaignTotals{credited: 1, purchasing: 1, revenue: 129, assisted: 1}
		if a.sums[campaignDay{2, 7}] != wantWinner || a.sums[campaignDay{1, 7}].assisted != 1 ||
			a.sums[campaignDay{3, 7}].assisted != 1 || len(a.sums) != 3 {
			t.Fatalf("single=%v: wrong campaign totals %v", single, a.sums)
		}
	}
}
