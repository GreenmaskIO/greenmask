package driver

import (
	"fmt"

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

func decodeGeometry(buf []byte) (any, error) {
	return string(buf), nil
}

func decodeDecimal(buf []byte) (any, error) {
	return decimal.NewFromString(string(buf))
}
