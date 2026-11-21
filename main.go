package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

const rowGroupSize = 64 << 10
const partPrefix = "shopping-sessions-"

type tableWriter[T any] struct {
	file  *os.File
	path  string
	rows  []T
	write *parquet.GenericWriter[T]
}

func openTable[T any](dir string, part int) (*tableWriter[T], error) {
	file, err := os.CreateTemp(dir, ".shopping-sessions-*.tmp")
	if err != nil {
		return nil, err
	}
	return &tableWriter[T]{
		file: file,
		path: filepath.Join(dir, fmt.Sprintf("%s%05d.parquet", partPrefix, part)),
		rows: make([]T, 0, rowGroupSize),
		write: parquet.NewGenericWriter[T](file,
			parquet.Compression(&snappy.Codec{}),
			parquet.MaxRowsPerRowGroup(rowGroupSize),
		),
	}, nil
}

func (w *tableWriter[T]) add(row T) error {
	w.rows = append(w.rows, row)
	if len(w.rows) == cap(w.rows) {
		return w.flush()
	}
	return nil
}

func (w *tableWriter[T]) flush() error {
	if len(w.rows) == 0 {
		return nil
	}
	if _, err := w.write.Write(w.rows); err != nil {
		return err
	}
	w.rows = w.rows[:0]
	return nil
}

func (w *tableWriter[T]) finish() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := errors.Join(w.write.Close(), w.file.Chmod(0o644), w.file.Close()); err != nil {
		return err
	}
	return os.Rename(w.file.Name(), w.path)
}

func (w *tableWriter[T]) cleanup() {
	w.file.Close()
	os.Remove(w.file.Name())
}

func prepareTable(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		stem, extOK := strings.CutSuffix(entry.Name(), ".parquet")
		part, prefixOK := strings.CutPrefix(stem, partPrefix)
		if !entry.IsDir() && extOK && prefixOK && len(part) == 5 {
			if _, ok := asciiNumber(part, 99_999); ok {
				if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func writeTables(out string, workers int, events []partition[event], clicks []partition[click], ids *identities, c catalog) error {
	userDir, campaignDir := filepath.Join(out, "user_daily"), filepath.Join(out, "campaign_daily")
	if err := prepareTable(userDir); err != nil {
		return err
	}
	if err := prepareTable(campaignDir); err != nil {
		return err
	}

	states := make([]aggregator, workers)
	for i := range states {
		writer, err := openTable[userDaily](userDir, i)
		if err != nil {
			return err
		}
		defer writer.cleanup()
		states[i] = aggregator{
			userRows: writer,
			ids:      ids,
			names:    c.categories,
			revenue:  make([]int64, len(c.categories)),
			ads: attribution{
				sums:   make(map[campaignDay]campaignTotals),
				states: make([]activeCampaign, len(c.campaigns)),
			},
		}
	}
	failures := make([]error, workers)
	parallel(workers, bucketCount, func(worker, bucket int) {
		if failures[worker] != nil {
			return
		}
		ev := takeBlocks(events, bucket).flatten()
		cl := takeBlocks(clicks, bucket).flatten()
		failures[worker] = states[worker].process(ev, cl)
	})
	if err := errors.Join(failures...); err != nil {
		return err
	}
	parallel(workers, workers, func(_, part int) {
		failures[part] = states[part].userRows.finish()
	})
	if err := errors.Join(failures...); err != nil {
		return err
	}
	return writeCampaigns(campaignDir, c.campaigns, states)
}

func writeCampaigns(dir string, campaigns []campaign, states []aggregator) error {
	totals := make(map[campaignDay]campaignTotals)
	for _, state := range states {
		for k, v := range state.ads.sums {
			t := totals[k]
			t.credited += v.credited
			t.purchasing += v.purchasing
			t.revenue += v.revenue
			t.assisted += v.assisted
			totals[k] = t
		}
	}
	writer, err := openTable[campaignDaily](dir, 0)
	if err != nil {
		return err
	}
	defer writer.cleanup()
	for key, total := range totals {
		c := campaigns[key.campaign]
		if err := writer.add(campaignDaily{
			CampaignID: c.id, Channel: c.channel, Day: key.day,
			Sessions: total.credited, PurchasingSessions: total.purchasing,
			RevenueCents: total.revenue, AssistedSessions: total.assisted,
		}); err != nil {
			return err
		}
	}
	return writer.finish()
}

func run(data, out string, workers int) error {
	started := time.Now()
	w, err := readWindow(data)
	if err != nil {
		return err
	}
	c, err := readCatalog(data)
	if err != nil {
		return err
	}
	ids := &identities{}
	a, err := readAliases(filepath.Join(data, "links"), ids)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "catalogs and links: %v\n", time.Since(started).Round(time.Millisecond))
	stage := time.Now()
	ev, err := readEvents(filepath.Join(data, "events"), workers, w, ids, a, c)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "events and deduplication: %v\n", time.Since(stage).Round(time.Millisecond))
	stage = time.Now()
	cl, err := readClicks(filepath.Join(data, "clicks"), workers, w, ids, a, c)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "clicks: %v\n", time.Since(stage).Round(time.Millisecond))
	stage = time.Now()
	if err := writeTables(out, workers, ev, cl, ids, c); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "sessions, attribution and Parquet: %v (total: %v)\n",
		time.Since(stage).Round(time.Millisecond), time.Since(started).Round(time.Millisecond))
	return nil
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func positiveInt(name string, fallback int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return fallback
}

func main() {
	data, out := getenv("DATA_DIR", "/data"), getenv("OUTPUT_DIR", "/output")
	switch len(os.Args) {
	case 1:
	case 3:
		data, out = os.Args[1], os.Args[2]
	default:
		fmt.Fprintln(os.Stderr, "usage: shopping-sessions [DATA_DIR OUTPUT_DIR]")
		os.Exit(2)
	}
	workers := min(positiveInt("NCPU", 8), runtime.NumCPU())
	runtime.GOMAXPROCS(workers)
	debug.SetMemoryLimit(int64(positiveInt("MEM_GB", 24)) * (1 << 30) * 3 / 4)
	if err := run(data, out, workers); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
