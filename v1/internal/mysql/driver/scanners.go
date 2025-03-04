package driver

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

func scanJson(data []byte, dest interface{}) error {
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

func scanTimestamp(data []byte, dest interface{}, loc *time.Location) error {
	if dest == nil {
		return errors.New("destination is nil")
	}

	switch v := dest.(type) {
	case *string:
		*v = string(data)
	case *time.Time:
		parsedTime, err := parseDateTime(data, loc)
		if err != nil {
			return err
		}
		*v = parsedTime
	default:
		return fmt.Errorf("cannot scan timestamp into destination %T", dest)
	}
	return nil
}

func scanInt64(data []byte, dest interface{}) error {
	if dest == nil {
		return errors.New("destination is nil")
	}

	switch v := dest.(type) {
	case *int64:
		parsedInt, err := strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return err
		}
		*v = parsedInt
	case *uint64:
		parsedUint, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return err
		}
		*v = parsedUint
	default:
		return fmt.Errorf("cannot scan int64 into destination %T", dest)
	}
	return nil
}

func scanBool(data []byte, dest interface{}) error {
	if dest == nil {
		return errors.New("destination is nil")
	}

	switch v := dest.(type) {
	case *bool:
		parsedBool := string(data) == "1"
		*v = parsedBool
	default:
		return fmt.Errorf("cannot scan bool into destination %T", dest)
	}
	return nil
}

func scanFloat(data []byte, dest interface{}) error {
	if dest == nil {
		return errors.New("destination is nil")
	}

	switch v := dest.(type) {
	case *float64:
		parsedFloat, err := strconv.ParseFloat(string(data), 64)
		if err != nil {
			return err
		}
		*v = parsedFloat
	case *float32:
		parsedFloat, err := strconv.ParseFloat(string(data), 32)
		if err != nil {
			return err
		}
		*v = float32(parsedFloat)
	default:
		return fmt.Errorf("cannot scan float into destination %T", dest)
	}
	return nil
}

func scanString(data []byte, dest interface{}) error {
	if dest == nil {
		return errors.New("destination is nil")
	}

	switch v := dest.(type) {
	case *string:
		*v = string(data)
	case *[]byte:
		*v = data
	default:
		return fmt.Errorf("cannot scan string into destination %T", dest)
	}
	return nil
}

func scanBinary(data []byte, dest interface{}) error {
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

func scanEnum(data []byte, dest interface{}) error {
	return scanString(data, dest)
}

func scanGeometry(data []byte, dest interface{}) error {
	return scanString(data, dest)
}

func scanDecimal(data []byte, dest interface{}) error {
	switch v := dest.(type) {
	case *string:
		*v = string(data)
	case *[]byte:
		*v = data
	case *decimal.Decimal:
		vv, err := decimal.NewFromString(string(data))
		if err != nil {
			return err
		}
		*v = vv
	}
	return fmt.Errorf("cannot scan decimal into destination %T", dest)
}
