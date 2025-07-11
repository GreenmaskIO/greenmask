package dbmsdriver

import (
	"fmt"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

func decodeBool(buf []byte) (any, error) {
	switch string(buf) {
	case "1", "true", "TRUE", "True":
		return true, nil
	case "0", "false", "FALSE", "False":
		return false, nil
	default:
		return nil, fmt.Errorf("cannot decode %q as bool", buf)
	}
}

func decodeEnum(buf []byte) (any, error) {
	return string(buf), nil
}

func decodeDecimal(buf []byte) (any, error) {
	return decimal.NewFromString(string(buf))
}

func decodeBit(buf []byte) (any, error) {
	return strconv.ParseInt(string(buf), 10, 8)
}

func decodeTime(buf []byte) (any, error) {
	t, err := time.Parse("15:04:05", string(buf))
	if err != nil {
		return 0, err
	}
	return time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second, nil
}
