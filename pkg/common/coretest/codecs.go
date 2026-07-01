// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coretest

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// loc is the time zone used for date-time encode/decode/scan. It mirrors the
// time zone the production engine drivers use (the host's local location), so
// the canonical wire round-trip agrees with transformers that mint values via
// time.Unix (which are Local-located): a value truncated to a calendar
// boundary in Local must re-encode to that same boundary, not shift across it.
var loc = time.Local

// The date-time wire format mirrors the canonical "YYYY-MM-DD HH:MM:SS.ffffff"
// representation produced by the relational engines greenmask supports, so the
// common transformer tests assert against a single stable layout.

func encodeString(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	default:
		return nil, fmt.Errorf("cannot encode %T as string", v)
	}
	return buf, nil
}

func decodeString(src []byte) (any, error) {
	return string(src), nil
}

func scanString(data []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	switch v := dest.(type) {
	case *string:
		*v = string(data)
	case *[]byte:
		*v = data
	case *uuid.UUID:
		parsed, err := uuid.ParseBytes(data)
		if err != nil {
			return err
		}
		*v = parsed
	default:
		return fmt.Errorf("cannot scan string into destination %T", dest)
	}
	return nil
}

func encodeBinary(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	default:
		return nil, fmt.Errorf("cannot encode %T as binary", v)
	}
	return buf, nil
}

func decodeBinary(src []byte) (any, error) {
	return src, nil
}

func scanBinary(data []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	switch v := dest.(type) {
	case *[]byte:
		*v = data
	default:
		return fmt.Errorf("cannot scan binary into destination %T", dest)
	}
	return nil
}

func encodeInt64(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case int64:
		buf = strconv.AppendInt(buf, v, 10)
	case uint64:
		buf = strconv.AppendUint(buf, v, 10)
	default:
		return nil, fmt.Errorf("cannot encode %T as int64", v)
	}
	return buf, nil
}

func decodeInt64(src []byte) (any, error) {
	return strconv.ParseInt(string(src), 10, 64)
}

// decodeUint64 strictly decodes an unsigned integer to uint64 for every value,
// so an unsigned column yields uint64 regardless of magnitude. It mirrors the
// real engine drivers' strict, non-value-dependent integer typing.
func decodeUint64(src []byte) (any, error) {
	return strconv.ParseUint(string(src), 10, 64)
}

func scanInt64(data []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	switch v := dest.(type) {
	case *int64:
		parsed, err := strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return err
		}
		*v = parsed
	case *uint64:
		parsed, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return err
		}
		*v = parsed
	default:
		return fmt.Errorf("cannot scan int64 into destination %T", dest)
	}
	return nil
}

func encodeFloat(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case float64:
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
	case float32:
		buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
	default:
		return nil, fmt.Errorf("cannot encode %T as float64", v)
	}
	return buf, nil
}

func decodeFloat(src []byte) (any, error) {
	return strconv.ParseFloat(string(src), 64)
}

func scanFloat(data []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	switch v := dest.(type) {
	case *float64:
		parsed, err := strconv.ParseFloat(string(data), 64)
		if err != nil {
			return err
		}
		*v = parsed
	case *float32:
		parsed, err := strconv.ParseFloat(string(data), 32)
		if err != nil {
			return err
		}
		*v = float32(parsed)
	default:
		return fmt.Errorf("cannot scan float into destination %T", dest)
	}
	return nil
}

func encodeDecimal(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case float64:
		buf = strconv.AppendFloat(buf, v, 'f', -1, 64)
	case float32:
		buf = strconv.AppendFloat(buf, float64(v), 'f', -1, 32)
	case int64:
		buf = strconv.AppendInt(buf, v, 10)
	case int:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case uint64:
		buf = strconv.AppendUint(buf, v, 10)
	case uint:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case decimal.Decimal:
		buf = append(buf, v.String()...)
	default:
		return nil, fmt.Errorf("cannot encode %T as decimal", v)
	}
	return buf, nil
}

func decodeDecimal(src []byte) (any, error) {
	return decimal.NewFromString(string(src))
}

func scanDecimal(src []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	strVal := string(src)
	switch d := dest.(type) {
	case *string:
		*d = strVal
	case *[]byte:
		*d = append((*d)[:0], src...)
	case *float64:
		val, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return fmt.Errorf("scanDecimal: parse float64: %w", err)
		}
		*d = val
	case *float32:
		val, err := strconv.ParseFloat(strVal, 32)
		if err != nil {
			return fmt.Errorf("scanDecimal: parse float32: %w", err)
		}
		*d = float32(val)
	case *decimal.Decimal:
		dec, err := decimal.NewFromString(strVal)
		if err != nil {
			return fmt.Errorf("scanDecimal: parse decimal: %w", err)
		}
		*d = dec
	default:
		return fmt.Errorf("unsupported scan destination for decimal: %T", dest)
	}
	return nil
}

func encodeBool(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case bool:
		if v {
			buf = append(buf, '1')
		} else {
			buf = append(buf, '0')
		}
	default:
		return nil, fmt.Errorf("cannot encode %T as bool", v)
	}
	return buf, nil
}

func decodeBool(src []byte) (any, error) {
	switch string(src) {
	case "1", "true", "TRUE", "True":
		return true, nil
	case "0", "false", "FALSE", "False":
		return false, nil
	default:
		return nil, fmt.Errorf("cannot decode %q as bool", src)
	}
}

func scanBool(data []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	switch v := dest.(type) {
	case *bool:
		*v = string(data) == "1" || string(data) == "true" || string(data) == "TRUE" || string(data) == "True"
	default:
		return fmt.Errorf("cannot scan bool into destination %T", dest)
	}
	return nil
}

func encodeJson(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	default:
		return nil, fmt.Errorf("cannot encode %T as json", v)
	}
	return buf, nil
}

func decodeJson(src []byte) (any, error) {
	return string(src), nil
}

func scanJson(data []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	switch v := dest.(type) {
	case *string:
		*v = string(data)
	case *[]byte:
		*v = data
	default:
		return fmt.Errorf("cannot scan json into destination %T", dest)
	}
	return nil
}

func encodeTimestamp(v any, buf []byte) ([]byte, error) {
	var err error
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case time.Time:
		if v.IsZero() {
			buf = append(buf, "0000-00-00 00:00:00"...)
		} else {
			buf, err = appendDateTime(buf, v.In(loc), time.Microsecond)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("cannot encode %T as timestamp/date", v)
	}
	return buf, nil
}

func decodeTimestamp(src []byte) (any, error) {
	return parseDateTime(src, loc)
}

func scanTimestamp(data []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	switch v := dest.(type) {
	case *string:
		*v = string(data)
	case *time.Time:
		parsed, err := parseDateTime(data, loc)
		if err != nil {
			return err
		}
		*v = parsed
	default:
		return fmt.Errorf("cannot scan timestamp into destination %T", dest)
	}
	return nil
}

func encodeTime(src any, buf []byte) ([]byte, error) {
	var d time.Duration
	switch v := src.(type) {
	case time.Duration:
		d = v
	case int64:
		d = time.Duration(v)
	case string:
		t, err := time.Parse("15:04:05", v)
		if err != nil {
			return nil, fmt.Errorf("invalid time string format: %w", err)
		}
		d = time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second
	default:
		return nil, fmt.Errorf("unsupported type for time: %T", src)
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return append(buf, fmt.Sprintf("%02d:%02d:%02d", h, m, s)...), nil
}

func decodeTime(src []byte) (any, error) {
	t, err := time.Parse("15:04:05", string(src))
	if err != nil {
		return time.Duration(0), err
	}
	return time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second, nil
}

func scanTime(src []byte, dest any) error {
	if dest == nil {
		return errors.New("destination is nil")
	}
	parsed, err := time.Parse("15:04:05", string(src))
	if err != nil {
		return fmt.Errorf("invalid time format: %w", err)
	}
	duration := time.Duration(parsed.Hour())*time.Hour +
		time.Duration(parsed.Minute())*time.Minute +
		time.Duration(parsed.Second())*time.Second
	switch d := dest.(type) {
	case *time.Duration:
		*d = duration
	case *int64:
		*d = int64(duration)
	case *string:
		*d = string(src)
	default:
		return fmt.Errorf("unsupported scan destination for time: %T", dest)
	}
	return nil
}

func encodeUUID(v any, buf []byte) ([]byte, error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case uuid.UUID:
		buf = append(buf, v.String()...)
	default:
		return nil, fmt.Errorf("cannot encode %T as uuid", v)
	}
	return buf, nil
}

func decodeUUID(src []byte) (any, error) {
	return uuid.ParseBytes(src)
}

func scanUUID(src []byte, dest any) error {
	return scanString(src, dest)
}

/******************************************************************************
 *               Date-time parsing/formatting helpers                          *
 * Adapted from the Go-MySQL-Driver project (github.com/go-sql-driver/mysql),  *
 * MPL 2.0. The canonical "YYYY-MM-DD HH:MM:SS.ffffff" wire layout is shared   *
 * across the relational engines greenmask supports.                           *
 ******************************************************************************/

var isoLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02",
}

func parseISODate(s string) (time.Time, error) {
	for _, layout := range isoLayouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported date format: %q", s)
}

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
		res, err := parseISODate(string(b))
		if err != nil {
			return time.Time{}, fmt.Errorf("bad value for field: `%s`", b)
		}
		return res, nil
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
		return buf, errors.New("year is not in the range [1, 9999]: " + strconv.Itoa(year))
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
