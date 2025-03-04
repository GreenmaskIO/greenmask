package driver

import (
	"fmt"
	"strconv"
	"time"
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
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case time.Time:
		if v.IsZero() {
			buf = append(buf, "'0000-00-00'"...)
		} else {
			buf = append(buf, '\'')
			buf, err := appendDateTime(buf, v.In(loc), 0)
			if err != nil {
				return nil, err
			}
			buf = append(buf, '\'')
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
	default:
		return nil, fmt.Errorf("cannot encode %T as decimal", v)
	}
	return buf, nil
}
