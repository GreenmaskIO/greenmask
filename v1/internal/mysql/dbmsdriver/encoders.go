package dbmsdriver

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

var (
	errBitValueIsOurOfRange = errors.New("bit value is our of range")
)

func encodeJson(v interface{}, buf []byte) ([]byte, error) {
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

func encodeTimestamp(v interface{}, buf []byte, loc *time.Location) ([]byte, error) {
	var err error
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case time.Time:
		if v.IsZero() {
			buf = append(buf, "'0000-00-00'"...)
		} else {
			buf, err = appendDateTime(buf, v.In(loc), time.Millisecond)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("cannot encode %T as timestamp/timestamptz/date", v)
	}
	return buf, nil
}

func encodeInt64(v interface{}, buf []byte) ([]byte, error) {
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

func encodeBool(v interface{}, buf []byte) ([]byte, error) {
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

func encodeFloat(v interface{}, buf []byte) ([]byte, error) {
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

func encodeString(v interface{}, buf []byte) ([]byte, error) {
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

func encodeBinary(v interface{}, buf []byte) ([]byte, error) {
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

func encodeEnum(v interface{}, buf []byte) ([]byte, error) {
	return encodeString(v, buf)
}

func encodeGeometry(v interface{}, buf []byte) ([]byte, error) {
	return encodeString(v, buf)
}

func encodeDecimal(v interface{}, buf []byte) ([]byte, error) {
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

func encodeBit(v interface{}, buf []byte) ([]byte, error) {
	var castedValue int64
	switch vv := v.(type) {
	case byte:
		buf = append(buf, vv)
	case int:
		castedValue = int64(vv)
	case int64:
		castedValue = int64(vv)
	case int32:
		castedValue = int64(vv)
	case int16:
		castedValue = int64(vv)
	case int8:
		castedValue = int64(vv)
	case uint64:
		if vv > math.MaxInt64 {
			return nil, errBitValueIsOurOfRange
		}
		castedValue = int64(vv)
	case uint32:
		castedValue = int64(vv)
	case uint16:
		castedValue = int64(vv)
	}
	if castedValue < 0 || castedValue > 1 {
		return nil, errBitValueIsOurOfRange
	}
	return strconv.AppendInt(buf, castedValue, 10), nil
}

func encodeTime(src any, buf []byte) ([]byte, error) {
	var d time.Duration

	switch v := src.(type) {
	case time.Duration:
		d = v
	case int64:
		d = time.Duration(v)
	case string:
		// Allow users to provide "12:30:45"
		t, err := time.Parse("15:04:05", v)
		if err != nil {
			return nil, fmt.Errorf("invalid time string format: %w", err)
		}
		d = time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second
	default:
		return nil, fmt.Errorf("unsupported type for TIME: %T", src)
	}

	// Convert duration to HH:MM:SS
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	formatted := fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	return append(buf, formatted...), nil
}
