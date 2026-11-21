package main

import (
	"encoding/json"
	"fmt"
	"math"
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
	start, startOK := timestamp(manifest.Start)
	end, endOK := timestamp(manifest.End)
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
func scanParts(dir string, workers int, visit func(int, string)) error {
	paths, err := csvPaths(dir)
	if err != nil {
		return err
	}
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
		lines(jobs[job], func(line string) {
			visit(worker, line)
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

func twoDigits(a, b byte) int64 {
	a, b = a-'0', b-'0'
	if a > 9 || b > 9 {
		return -1
	}
	return 10*int64(a) + int64(b)
}

func timestamp(s string) (int64, bool) {
	if len(s) >= 5 && s[4] == '-' {
		return calendarTimestamp(s)
	}
	return asciiNumber(s, math.MaxInt64)
}

func calendarTimestamp(s string) (int64, bool) {
	if len(s) != 20 && len(s) != 25 {
		return 0, false
	}
	if s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' {
		return 0, false
	}
	century, yy := twoDigits(s[0], s[1]), twoDigits(s[2], s[3])
	month, day := twoDigits(s[5], s[6]), twoDigits(s[8], s[9])
	hour, minute, second := twoDigits(s[11], s[12]), twoDigits(s[14], s[15]), twoDigits(s[17], s[18])
	if century < 0 || yy < 0 || month < 1 || month > 12 || day < 1 ||
		hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return 0, false
	}
	year := century*100 + yy
	leap := year%4 == 0 && (yy != 0 || century%4 == 0)
	monthLength := [...]int64{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	lastDay := monthLength[month-1]
	if month == 2 && leap {
		lastDay++
	}
	if day > lastDay {
		return 0, false
	}
	var offset int64
	if len(s) == 20 {
		if s[19] != 'Z' {
			return 0, false
		}
	} else {
		oh, om := twoDigits(s[20], s[21]), twoDigits(s[23], s[24])
		if (s[19] != '+' && s[19] != '-') || s[22] != ':' ||
			oh < 0 || oh > 23 || om < 0 || om > 59 {
			return 0, false
		}
		offset = 60*oh + om
		if s[19] == '-' {
			offset = -offset
		}
	}
	// Day zero is 1970-01-01. Count leap days before the given year.
	completeYears := year - 1
	days := completeYears*365 + floorDiv(completeYears, 4) -
		floorDiv(completeYears, 100) + floorDiv(completeYears, 400) - 719162
	beforeMonth := [...]int64{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}
	days += beforeMonth[month-1] + day - 1
	if leap && month > 2 {
		days++
	}
	return ((days*24+hour)*3600 + (minute-offset)*60 + second) * 1000, true
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor < 0 {
		quotient--
	}
	return quotient
}
