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

package dbmsdriver

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
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
	case *uuid.UUID:
		parsedUUID, err := uuid.ParseBytes(data)
		if err != nil {
			return err
		}
		*v = parsedUUID
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

func scanTime(src []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("destination is nil")
	}

	parsed, err := time.Parse("15:04:05", string(src))
	if err != nil {
		return fmt.Errorf("invalid TIME format: %w", err)
	}

	duration := time.Duration(parsed.Hour())*time.Hour +
		time.Duration(parsed.Minute())*time.Minute +
		time.Duration(parsed.Second())*time.Second

	switch d := dest.(type) {
	case *time.Duration:
		*d = duration
		return nil
	case *int64:
		*d = int64(duration)
		return nil
	case *string:
		*d = string(src)
		return nil
	default:
		return fmt.Errorf("unsupported scan destination for TIME: %T", dest)
	}
}

func scanBit(src []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("destination is nil")
	}

	// Parse the string to int64 (expecting "0" or "1")
	val, err := strconv.ParseInt(string(src), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid BIT format: %w", err)
	}
	if val != 0 && val != 1 {
		return fmt.Errorf("BIT value out of range: %d", val)
	}

	switch d := dest.(type) {
	case *bool:
		*d = val == 1
	case *int64:
		*d = val
	case *int:
		*d = int(val)
	case *int8:
		*d = int8(val)
	case *int16:
		*d = int16(val)
	case *int32:
		*d = int32(val)
	case *uint:
		*d = uint(val)
	case *uint8:
		*d = uint8(val)
	case *uint16:
		*d = uint16(val)
	case *uint32:
		*d = uint32(val)
	case *uint64:
		*d = uint64(val)
	case *string:
		*d = string(src)
	case *[]byte:
		*d = append((*d)[:0], src...)
	default:
		return fmt.Errorf("unsupported scan destination for BIT: %T", dest)
	}

	return nil
}

func scanDecimal(src []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("destination is nil")
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
		return fmt.Errorf("unsupported scan destination for DECIMAL: %T", dest)
	}

	return nil
}
