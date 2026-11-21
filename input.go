package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	dayMillis     = 86_400_000
	clickLookback = 7 * dayMillis
	sessionGap    = 30 * 60_000
	chunkBytes    = 8 << 20
)

type window struct {
	start, end int64
}

func readWindow(dir string) (window, error) {
	var manifest struct {
		Start string `json:"window_start"`
		End   string `json:"window_end"`
	}
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return window{}, err
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return window{}, fmt.Errorf("manifest: %w", err)
	}
	var decoder timestampDecoder
	start, startOK := decoder.timestamp(manifest.Start)
	end, endOK := decoder.timestamp(manifest.End)
	if !startOK || !endOK || end <= start {
		return window{}, fmt.Errorf("invalid manifest window")
	}
	return window{start, end}, nil
}

// The dialect has no escaped quotes and no embedded separators. Keep CR and spaces.
func fields(line string, result []string) bool {
	for i := range result {
		var field string
		if i == len(result)-1 {
			if strings.IndexByte(line, ',') >= 0 {
				return false
			}
			field = line
		} else {
			var found bool
			field, line, found = strings.Cut(line, ",")
			if !found {
				return false
			}
		}
		if len(field) > 0 && field[0] == '"' {
			if len(field) < 2 || field[len(field)-1] != '"' {
				return false
			}
			field = field[1 : len(field)-1]
		}
		if strings.IndexByte(field, '"') >= 0 {
			return false
		}
		result[i] = field
	}
	return true
}

func lines(text string, visit func(string)) {
	for len(text) != 0 {
		line, rest, _ := strings.Cut(text, "\n")
		visit(line)
		text = rest
	}
}

func csvPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".csv") {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}
	return paths, nil
}

func mapFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() == 0 {
		return nil, err
	}
	return unix.Mmap(int(file.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
}

// Divide large files at newline boundaries so a single large part can use all workers.
// The visitor must copy any strings it keeps after the job finishes.
func scanParts(dir string, workers, width int, visit func(int, []string)) error {
	paths, err := csvPaths(dir)
	if err != nil {
		return err
	}
	return scanPaths(paths, workers, width, visit)
}

func scanPaths(paths []string, workers, width int, visit func(int, []string)) error {
	var mappings [][]byte
	defer func() {
		for _, data := range mappings {
			unix.Munmap(data)
		}
	}()
	var jobs []string
	for _, path := range paths {
		data, err := mapFile(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if len(data) == 0 {
			continue
		}
		mappings = append(mappings, data)
		text := unsafe.String(&data[0], len(data))
		_, text, _ = strings.Cut(text, "\n")
		for len(text) > chunkBytes {
			next := strings.IndexByte(text[chunkBytes:], '\n')
			if next < 0 {
				break
			}
			end := chunkBytes + next + 1
			jobs = append(jobs, text[:end])
			text = text[end:]
		}
		if text != "" {
			jobs = append(jobs, text)
		}
	}
	parallel(workers, len(jobs), func(worker, job int) {
		scanCSV(jobs[job], width, func(record []string) {
			visit(worker, record)
		})
	})
	return nil
}

func parallel(workers, count int, run func(worker, job int)) {
	var next atomic.Int64
	var group sync.WaitGroup
	for worker := 0; worker < min(workers, count); worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for {
				job := int(next.Add(1)) - 1
				if job >= count {
					return
				}
				run(worker, job)
			}
		}(worker)
	}
	group.Wait()
}

func asciiNumber(s string, limit int64) (int64, bool) {
	if s == "" {
		return 0, false
	}
	var value int64
	cutoff, lastDigit := limit/10, limit%10
	for i := 0; i < len(s); i++ {
		d := int64(s[i] - '0')
		if d > 9 || value > cutoff || (value == cutoff && d > lastDigit) {
			return 0, false
		}
		value = 10*value + d
	}
	return value, true
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor < 0 {
		quotient--
	}
	return quotient
}
