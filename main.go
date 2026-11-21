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
	"sync"
	"time"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"golang.org/x/sys/unix"
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

func writeTables(out string, workers int, events []partition[event], clicks []partition[click], ids *identities, c catalog, w window) error {
	userDir, campaignDir := filepath.Join(out, "user_daily"), filepath.Join(out, "campaign_daily")
	if err := prepareTable(userDir); err != nil {
		return err
	}
	if err := prepareTable(campaignDir); err != nil {
		return err
	}

	firstDay := floorDiv(w.start, dayMillis)
	lastDay := floorDiv(w.end-1, dayMillis)
	days := lastDay - firstDay + 1
	campaignCount := len(c.campaigns)
	useDense := days <= 65_536/int64(campaignCount) &&
		firstDay == int64(int32(firstDay)) && lastDay == int64(int32(lastDay))
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
				states:   make([]activeCampaign, campaignCount),
				firstDay: int32(firstDay),
			},
		}
		// Keep the hot campaign/day cells contiguous for small windows.
		if useDense {
			states[i].ads.dense = make([]campaignTotals, int(days)*campaignCount)
		} else {
			states[i].ads.sums = make(map[campaignDay]campaignTotals)
		}
	}
	failures := make([]error, workers)
	parallel(workers, bucketCount, func(worker, bucket int) {
		if failures[worker] != nil {
			return
		}
		ev := takeBlocks(events, bucket)
		cl := takeBlocks(clicks, bucket)
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
	merge := func(k campaignDay, v campaignTotals) {
		total := totals[k]
		total.add(v)
		totals[k] = total
	}
	for _, state := range states {
		for i, v := range state.ads.dense {
			if v.credited != 0 || v.assisted != 0 {
				k := campaignDay{uint32(i % len(campaigns)), state.ads.firstDay + int32(i/len(campaigns))}
				merge(k, v)
			}
		}
		for k, v := range state.ads.sums {
			merge(k, v)
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
	var a aliases
	var aliasErr error
	var pending sync.WaitGroup
	pending.Add(1)
	go func() {
		defer pending.Done()
		a, aliasErr = readAliases(filepath.Join(data, "links"), workers, ids)
	}()
	raw, err := readEventCandidates(filepath.Join(data, "events"), workers, w, ids, c)
	pending.Wait()
	if err := errors.Join(err, aliasErr); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "catalogs, links and event scan: %v\n", time.Since(started).Round(time.Millisecond))
	stage := time.Now()
	var cl []partition[click]
	var clickErr error
	pending.Add(1)
	go func() {
		defer pending.Done()
		cl, clickErr = readClicks(filepath.Join(data, "clicks"), workers, w, ids, a, c)
	}()
	ev := deduplicate(raw, workers, a)
	pending.Wait()
	if clickErr != nil {
		return clickErr
	}
	fmt.Fprintf(os.Stderr, "event deduplication and click scan: %v\n", time.Since(stage).Round(time.Millisecond))
	stage = time.Now()
	if err := writeTables(out, workers, ev, cl, ids, c, w); err != nil {
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
	started := time.Now()
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
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) == nil {
		cpu := time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
		rss := float64(usage.Maxrss)
		if runtime.GOOS == "linux" {
			rss *= 1024
		}
		fmt.Fprintf(os.Stderr, "resources: %d workers, %.2f CPUs on average, %.2f GiB peak RSS\n",
			workers, float64(cpu)/float64(time.Since(started)), rss/(1<<30))
	}
}
