// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//
// This code was borrowed from the Go-MySQL-Driver project https://github.com/go-sql-driver/mysql

package dbmsdriver

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

/******************************************************************************
*                           Time related utils                                *
******************************************************************************/

func parseDateTime(b []byte, loc *time.Location) (time.Time, error) {
	const base = "0000-00-00 00:00:00.000000"
	switch len(b) {
	case 10, 19, 21, 22, 23, 24, 25, 26: // up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
		if string(b) == base[:len(b)] {
			return time.Time{}, nil
		}

		year, err := parseByteYear(b)
		if err != nil {
			return time.Time{}, err
		}
		if b[4] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[4])
		}

		m, err := parseByte2Digits(b[5], b[6])
		if err != nil {
			return time.Time{}, err
		}
		month := time.Month(m)

		if b[7] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[7])
		}

		day, err := parseByte2Digits(b[8], b[9])
		if err != nil {
			return time.Time{}, err
		}
		if len(b) == 10 {
			return time.Date(year, month, day, 0, 0, 0, 0, loc), nil
		}

		if b[10] != ' ' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[10])
		}

		hour, err := parseByte2Digits(b[11], b[12])
		if err != nil {
			return time.Time{}, err
		}
		if b[13] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[13])
		}

		min, err := parseByte2Digits(b[14], b[15])
		if err != nil {
			return time.Time{}, err
		}
		if b[16] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[16])
		}

		sec, err := parseByte2Digits(b[17], b[18])
		if err != nil {
			return time.Time{}, err
		}
		if len(b) == 19 {
			return time.Date(year, month, day, hour, min, sec, 0, loc), nil
		}

		if b[19] != '.' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[19])
		}
		nsec, err := parseByteNanoSec(b[20:])
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(year, month, day, hour, min, sec, nsec, loc), nil
	default:
		return time.Time{}, fmt.Errorf("invalid time bytes: %s", b)
	}
}

func parseByteYear(b []byte) (int, error) {
	year, n := 0, 1000
	for i := 0; i < 4; i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		year += v * n
		n /= 10
	}
	return year, nil
}

func parseByte2Digits(b1, b2 byte) (int, error) {
	d1, err := bToi(b1)
	if err != nil {
		return 0, err
	}
	d2, err := bToi(b2)
	if err != nil {
		return 0, err
	}
	return d1*10 + d2, nil
}

func parseByteNanoSec(b []byte) (int, error) {
	ns, digit := 0, 100000 // max is 6-digits
	for i := 0; i < len(b); i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		ns += v * digit
		digit /= 10
	}
	// nanoseconds has 10-digits. (needs to scale digits)
	// 10 - 6 = 4, so we have to multiple 1000.
	return ns * 1000, nil
}

func bToi(b byte) (int, error) {
	if b < '0' || b > '9' {
		return 0, errors.New("not [0-9]")
	}
	return int(b - '0'), nil
}

func appendDateTime(buf []byte, t time.Time, timeTruncate time.Duration) ([]byte, error) {
	if timeTruncate > 0 {
		t = t.Truncate(timeTruncate)
	}

	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()

	if year < 1 || year > 9999 {
		return buf, errors.New("year is not in the range [1, 9999]: " + strconv.Itoa(year)) // use errors.New instead of fmt.Errorf to avoid year escape to heap
	}
	year100 := year / 100
	year1 := year % 100

	var localBuf [len("2006-01-02T15:04:05.999999999")]byte // does not escape
	localBuf[0], localBuf[1], localBuf[2], localBuf[3] = digits10[year100], digits01[year100], digits10[year1], digits01[year1]
	localBuf[4] = '-'
	localBuf[5], localBuf[6] = digits10[month], digits01[month]
	localBuf[7] = '-'
	localBuf[8], localBuf[9] = digits10[day], digits01[day]

	if hour == 0 && min == 0 && sec == 0 && nsec == 0 {
		return append(buf, localBuf[:10]...), nil
	}

	localBuf[10] = ' '
	localBuf[11], localBuf[12] = digits10[hour], digits01[hour]
	localBuf[13] = ':'
	localBuf[14], localBuf[15] = digits10[min], digits01[min]
	localBuf[16] = ':'
	localBuf[17], localBuf[18] = digits10[sec], digits01[sec]

	if nsec == 0 {
		return append(buf, localBuf[:19]...), nil
	}
	nsec100000000 := nsec / 100000000
	nsec1000000 := (nsec / 1000000) % 100
	nsec10000 := (nsec / 10000) % 100
	nsec100 := (nsec / 100) % 100
	nsec1 := nsec % 100
	localBuf[19] = '.'

	// milli second
	localBuf[20], localBuf[21], localBuf[22] =
		digits01[nsec100000000], digits10[nsec1000000], digits01[nsec1000000]
	// micro second
	localBuf[23], localBuf[24], localBuf[25] =
		digits10[nsec10000], digits01[nsec10000], digits10[nsec100]
	// nano second
	localBuf[26], localBuf[27], localBuf[28] =
		digits01[nsec100], digits10[nsec1], digits01[nsec1]

	// trim trailing zeros
	n := len(localBuf)
	for n > 0 && localBuf[n-1] == '0' {
		n--
	}

	return append(buf, localBuf[:n]...), nil
}

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"
