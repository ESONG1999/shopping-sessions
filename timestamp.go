package main

import (
	"encoding/binary"
	"math"
)

type calendarMonth struct {
	prefix uint64
	start  int64
	days   int64
}

// The decoder only memoizes validated year/month prefixes. Time of day, day
// of month and the zone are checked independently for every timestamp.
type timestampDecoder struct {
	months [8]calendarMonth
}

func (d *timestampDecoder) timestamp(s string) (int64, bool) {
	if len(s) >= 5 && s[4] == '-' {
		return d.calendar(s)
	}
	return epochMillis(s)
}

func epochMillis(s string) (int64, bool) {
	if len(s) < 8 || len(s) > 18 {
		return asciiNumber(s, math.MaxInt64)
	}
	word := binary.LittleEndian.Uint64([]byte(s))
	const high, ascii = 0xf0f0f0f0f0f0f0f0, 0x3030303030303030
	if word&high != ascii || (word+0x0606060606060606)&high != ascii {
		return 0, false
	}
	// Combine adjacent decimal digits into pairs, then groups of four.
	word -= ascii
	word = (10*word + word>>8) & 0x00ff00ff00ff00ff
	word = (100*word + word>>16) & 0x0000ffff0000ffff
	value := int64(uint32(10000*word + word>>32))
	for i := 8; i < len(s); i++ {
		digit := s[i] - '0'
		if digit > 9 {
			return 0, false
		}
		value = 10*value + int64(digit)
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

func decodeMonth(s string, prefix uint64) calendarMonth {
	if s[4] != '-' || s[7] != '-' {
		return calendarMonth{}
	}
	century, yy := twoDigits(s[0], s[1]), twoDigits(s[2], s[3])
	month := twoDigits(s[5], s[6])
	if century < 0 || yy < 0 || month < 1 || month > 12 {
		return calendarMonth{}
	}
	year := century*100 + yy
	leap := year%4 == 0 && (yy != 0 || century%4 == 0)
	lengths := [...]int64{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	before := [...]int64{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}
	completeYears := year - 1
	// Day zero is 1970-01-01.
	days := completeYears*365 + floorDiv(completeYears, 4) -
		floorDiv(completeYears, 100) + floorDiv(completeYears, 400) - 719162
	days += before[month-1]
	last := lengths[month-1]
	if leap {
		switch {
		case month == 2:
			last++
		case month > 2:
			days++
		}
	}
	return calendarMonth{prefix, days * dayMillis, last}
}

func (d *timestampDecoder) calendar(s string) (int64, bool) {
	if len(s) != 20 && len(s) != 25 {
		return 0, false
	}
	if s[10] != 'T' || s[13] != ':' || s[16] != ':' {
		return 0, false
	}
	prefix := binary.LittleEndian.Uint64([]byte(s))
	month := &d.months[s[6]&7]
	if month.prefix != prefix || month.days == 0 {
		decoded := decodeMonth(s, prefix)
		if decoded.days == 0 {
			return 0, false
		}
		*month = decoded
	}
	day := twoDigits(s[8], s[9])
	hour, minute, second := twoDigits(s[11], s[12]), twoDigits(s[14], s[15]), twoDigits(s[17], s[18])
	if day < 1 || day > month.days || hour < 0 || hour > 23 ||
		minute < 0 || minute > 59 || second < 0 || second > 59 {
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
	return month.start + (day-1)*dayMillis + (hour*3600+(minute-offset)*60+second)*1000, true
}
