package main

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// EncodeMySQLValue encodes Go values into MySQL-compatible raw data
func EncodeMySQLValue(value any) (driver.Value, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case int, int8, int16, int32, int64:
		return v, nil
	case uint, uint8, uint16, uint32, uint64:
		return v, nil
	case float32, float64:
		return v, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		return v, nil
	case []byte:
		return v, nil
	case time.Time:
		return v.Format("2006-01-02 15:04:05"), nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", value)
	}
}

// DecodeMySQLValue decodes raw MySQL data into Go-compatible types
func DecodeMySQLValue(dataType string, rawValue any) (any, error) {
	if rawValue == nil {
		return nil, nil
	}

	switch dataType {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return decodeInteger(rawValue)
	case "float", "double", "real", "decimal", "numeric":
		return decodeFloat(rawValue)
	case "date", "datetime", "timestamp", "time", "year":
		return decodeTime(dataType, rawValue)
	case "char", "varchar", "text", "blob", "binary", "varbinary":
		return decodeString(rawValue)
	default:
		return nil, fmt.Errorf("unsupported MySQL type: %s", dataType)
	}
}

// ScanMySQLValue decodes and assigns a raw MySQL value to a Go variable (via pointer)
func ScanMySQLValue(dataType string, rawValue any, dest any) error {
	decoded, err := DecodeMySQLValue(dataType, rawValue)
	if err != nil {
		return err
	}

	switch d := dest.(type) {
	case *int:
		if v, ok := decoded.(int64); ok {
			*d = int(v)
			return nil
		}
	case *int64:
		if v, ok := decoded.(int64); ok {
			*d = v
			return nil
		}
	case *float64:
		if v, ok := decoded.(float64); ok {
			*d = v
			return nil
		}
	case *string:
		if v, ok := decoded.(string); ok {
			*d = v
			return nil
		}
	case *time.Time:
		if v, ok := decoded.(time.Time); ok {
			*d = v
			return nil
		}
	default:
		return fmt.Errorf("unsupported destination type: %T", dest)
	}

	return errors.New("type mismatch between raw value and destination")
}

// Helper functions for decoding
func decodeInteger(rawValue any) (int64, error) {
	switch v := rawValue.(type) {
	case int64:
		return v, nil
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported raw value type for integer: %T", rawValue)
	}
}

func decodeFloat(rawValue any) (float64, error) {
	switch v := rawValue.(type) {
	case float64:
		return v, nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	default:
		return 0, fmt.Errorf("unsupported raw value type for float: %T", rawValue)
	}
}

func decodeTime(dataType string, rawValue any) (time.Time, error) {
	layout := "2006-01-02 15:04:05" // Default for datetime/timestamp
	switch dataType {
	case "date":
		layout = "2006-01-02"
	case "time":
		layout = "15:04:05"
	case "year":
		layout = "2006"
	}

	switch v := rawValue.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return time.Parse(layout, string(v))
	default:
		return time.Time{}, fmt.Errorf("unsupported raw value type for time: %T", rawValue)
	}
}

func decodeString(rawValue any) (string, error) {
	switch v := rawValue.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("unsupported raw value type for string: %T", rawValue)
	}
}
