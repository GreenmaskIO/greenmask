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

package template

import (
	"fmt"
	"slices"
	"time"

	"github.com/spf13/cast"
)

const (
	SecUnixUnixName   = "sec"
	MilliUnixUnixName = "milli"
	MicroUnixUnixName = "micro"
	NanoUnixUnixName  = "nano"
)

var (
	UnixNanoToDate         = unixLikeToTimeLikeFuncMaker("date", NanoUnixUnixName)
	UnixMicroToDate        = unixLikeToTimeLikeFuncMaker("date", MicroUnixUnixName)
	UnixMilliToDate        = unixLikeToTimeLikeFuncMaker("date", MilliUnixUnixName)
	UnixSecToDate          = unixLikeToTimeLikeFuncMaker("date", SecUnixUnixName)
	UnixNanoToTimestamp    = unixLikeToTimeLikeFuncMaker("timestamp", NanoUnixUnixName)
	UnixMicroToTimestamp   = unixLikeToTimeLikeFuncMaker("timestamp", MicroUnixUnixName)
	UnixMilliToTimestamp   = unixLikeToTimeLikeFuncMaker("timestamp", MilliUnixUnixName)
	UnixSecToTimestamp     = unixLikeToTimeLikeFuncMaker("timestamp", SecUnixUnixName)
	UnixNanoToTimestampTz  = unixLikeToTimeLikeFuncMaker("timestamptz", NanoUnixUnixName)
	UnixMicroToTimestampTz = unixLikeToTimeLikeFuncMaker("timestamptz", MicroUnixUnixName)
	UnixMilliToTimestampTz = unixLikeToTimeLikeFuncMaker("timestamptz", MilliUnixUnixName)
	UnixSecToTimestampTz   = unixLikeToTimeLikeFuncMaker("timestamptz", SecUnixUnixName)

	DateToUnixNano         = timeLikeToUnixLikeFuncMaker("date", NanoUnixUnixName)
	DateToUnixMicro        = timeLikeToUnixLikeFuncMaker("date", MicroUnixUnixName)
	DateToUnixMilli        = timeLikeToUnixLikeFuncMaker("date", MilliUnixUnixName)
	DateToUnixSec          = timeLikeToUnixLikeFuncMaker("date", SecUnixUnixName)
	TimestampToUnixNano    = timeLikeToUnixLikeFuncMaker("timestamp", NanoUnixUnixName)
	TimestampToUnixMicro   = timeLikeToUnixLikeFuncMaker("timestamp", MicroUnixUnixName)
	TimestampToUnixMilli   = timeLikeToUnixLikeFuncMaker("timestamp", MilliUnixUnixName)
	TimestampToUnixSec     = timeLikeToUnixLikeFuncMaker("timestamp", SecUnixUnixName)
	TimestampTzToUnixNano  = timeLikeToUnixLikeFuncMaker("timestamptz", NanoUnixUnixName)
	TimestampTzToUnixMicro = timeLikeToUnixLikeFuncMaker("timestamptz", MicroUnixUnixName)
	TimestampTzToUnixMilli = timeLikeToUnixLikeFuncMaker("timestamptz", MilliUnixUnixName)
	TimestampTzToUnixSec   = timeLikeToUnixLikeFuncMaker("timestamptz", SecUnixUnixName)
)

var CastFunctionsMap = map[string]*TypeCastDefinition{
	"UnixNanoToDate": {
		Cast:        UnixNanoToDate,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"date"},
	},
	"UnixMicroToDate": {
		Cast:        UnixMicroToDate,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"date"},
	},
	"UnixMilliToDate": {
		Cast:        UnixMilliToDate,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"date"},
	},
	"UnixSecToDate": {
		Cast:        UnixSecToDate,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"date"},
	},

	"UnixNanoToTimestamp": {
		Cast:        UnixNanoToTimestamp,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamp"},
	},
	"UnixMicroToTimestamp": {
		Cast:        UnixMicroToTimestamp,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamp"},
	},
	"UnixMilliToTimestamp": {
		Cast:        UnixMilliToTimestamp,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamp"},
	},
	"UnixSecToTimestamp": {
		Cast:        UnixSecToTimestamp,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamp"},
	},

	"UnixNanoToTimestampTz": {
		Cast:        UnixNanoToTimestampTz,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamptz"},
	},
	"UnixMicroToTimestampTz": {
		Cast:        UnixMicroToTimestampTz,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamptz"},
	},
	"UnixMilliToTimestampTz": {
		Cast:        UnixMilliToTimestampTz,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamptz"},
	},
	"UnixSecToTimestampTz": {
		Cast:        UnixSecToTimestampTz,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"timestamptz"},
	},

	"DateToUnixNano": {
		Cast:        DateToUnixNano,
		InputTypes:  []string{"date"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"DateToUnixMicro": {
		Cast:        DateToUnixMicro,
		InputTypes:  []string{"date"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"DateToUnixMilli": {
		Cast:        DateToUnixMilli,
		InputTypes:  []string{"date"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"DateToUnixSec": {
		Cast:        DateToUnixSec,
		InputTypes:  []string{"date"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampToUnixNano": {
		Cast:        TimestampToUnixNano,
		InputTypes:  []string{"timestamp"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampToUnixMicro": {
		Cast:        TimestampToUnixMicro,
		InputTypes:  []string{"timestamp"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampToUnixMilli": {
		Cast:        TimestampToUnixMilli,
		InputTypes:  []string{"timestamp"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampToUnixSec": {
		Cast:        TimestampToUnixSec,
		InputTypes:  []string{"timestamp"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampTzToUnixNano": {
		Cast:        TimestampTzToUnixNano,
		InputTypes:  []string{"date"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampTzToUnixMicro": {
		Cast:        TimestampTzToUnixMicro,
		InputTypes:  []string{"timestamptz"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampTzToUnixMilli": {
		Cast:        TimestampTzToUnixMilli,
		InputTypes:  []string{"timestamptz"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
	"TimestampTzToUnixSec": {
		Cast:        TimestampTzToUnixSec,
		InputTypes:  []string{"timestamptz"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},

	"FloatToInt": {
		Cast:        CastFloatToInt,
		InputTypes:  []string{"numeric", "float4", "float8"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric"},
	},
	"IntToFloat": {
		Cast:        CastIntToFloat,
		InputTypes:  []string{"int2", "int4", "int8", "numeric"},
		OutputTypes: []string{"numeric", "float4", "float8"},
	},
	"IntToBool": {
		Cast:        IntToBool,
		InputTypes:  []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
		OutputTypes: []string{"bool"},
	},
	"BoolToInt": {
		Cast:        BoolToInt,
		InputTypes:  []string{"bool"},
		OutputTypes: []string{"int2", "int4", "int8", "numeric", "float4", "float8"},
	},
}

// TypeCastFunc the function implements type casting from one to another
type TypeCastFunc func(driver dbmsDriver, input []byte) (output []byte, err error)

type TypeCastDefinition struct {
	Cast        TypeCastFunc
	InputTypes  []string
	OutputTypes []string
}

func (tcd *TypeCastDefinition) ValidateTypes(inputType, outputType string) bool {
	// TODO: Check root base domain type
	return slices.Contains(tcd.InputTypes, inputType) && slices.Contains(tcd.OutputTypes, outputType)
}

func CastIntToFloat(driver dbmsDriver, input []byte) (output []byte, err error) {
	return input, nil
}

func CastFloatToInt(driver dbmsDriver, input []byte) (output []byte, err error) {
	floatVal, err := cast.ToFloat64E(string(input))
	if err != nil {
		return nil, fmt.Errorf("error decoding value from raw to float64: %w", err)
	}
	res, err := cast.ToStringE(int64(floatVal))
	if err != nil {
		return nil, fmt.Errorf("error encoding nt64 value to raw: %w", err)
	}
	return []byte(res), nil
}

func validateUnixTimeUnit(unit string) error {
	switch unit {
	case SecUnixUnixName, MilliUnixUnixName, MicroUnixUnixName, NanoUnixUnixName:
		return nil
	default:
		return fmt.Errorf("unknown unix time unit \"%s\"", unit)
	}
}

func timeToUnix(unit string, date time.Time) (int64, error) {
	if err := validateUnixTimeUnit(unit); err != nil {
		return 0, err
	}
	switch unit {
	case SecUnixUnixName:
		return date.Unix(), nil
	case MilliUnixUnixName:
		return date.UnixMilli(), nil
	case MicroUnixUnixName:
		return date.UnixMicro(), nil
	case NanoUnixUnixName:
		return date.UnixNano(), nil
	}
	return 0, nil
}

func unixToTime(unit string, value any) (time.Time, error) {
	if err := validateUnixTimeUnit(unit); err != nil {
		return time.Time{}, err
	}

	unixTime, err := cast.ToInt64E(value)
	if err != nil {
		return time.Time{}, fmt.Errorf("error casting %+v to int64: %w", unixTime, err)
	}

	switch unit {
	case SecUnixUnixName:
		return time.Unix(unixTime, 0), nil
	case MilliUnixUnixName:
		return time.UnixMilli(unixTime), nil
	case MicroUnixUnixName:
		return time.UnixMicro(unixTime), nil
	case NanoUnixUnixName:
		seconds := unixTime / int64(time.Second)
		nano := unixTime - seconds
		return time.Unix(seconds, nano), nil
	}
	return time.Time{}, nil
}

// Unix -> Date-like types cast

func unixLikeToTimeLikeFuncMaker(inputType, unixTimeUnit string) TypeCastFunc {
	allowedInputTypes := []string{"date", "timestamp", "timestamptz"}
	if !slices.Contains(allowedInputTypes, inputType) {
		panic(fmt.Sprintf("unknown input type \"%s\"", inputType))
	}
	if err := validateUnixTimeUnit(unixTimeUnit); err != nil {
		panic(err)
	}

	type unixTimeGenerator func(v int64) time.Time
	var generate unixTimeGenerator

	switch unixTimeUnit {
	case SecUnixUnixName:
		generate = func(v int64) time.Time {
			return time.Unix(v, 0)
		}
	case MilliUnixUnixName:
		generate = func(v int64) time.Time {
			return time.UnixMilli(v)
		}
	case MicroUnixUnixName:
		generate = func(v int64) time.Time {
			return time.UnixMicro(v)
		}
	case NanoUnixUnixName:
		generate = func(v int64) time.Time {
			seconds := v / int64(time.Second)
			nano := v - seconds
			return time.Unix(seconds, nano)
		}
	}

	return func(driver dbmsDriver, input []byte) (output []byte, err error) {
		unixTime, err := cast.ToInt64E(string(input))
		if err != nil {
			return nil, fmt.Errorf("error casting %+v to int64: %w", unixTime, err)
		}

		return driver.EncodeValueByTypeName(inputType, generate(unixTime), output)
	}
}

// Date-like -> Unix types cast
func timeLikeToUnixLikeFuncMaker(inputType, unixTimeUnit string) TypeCastFunc {
	allowedInputTypes := []string{"date", "timestamp", "timestamptz"}
	if !slices.Contains(allowedInputTypes, inputType) {
		panic(fmt.Sprintf("unknown input type \"%s\"", inputType))
	}
	if err := validateUnixTimeUnit(unixTimeUnit); err != nil {
		panic(err)
	}

	type unixTimeGenerator func(v time.Time) int64
	var generate unixTimeGenerator

	switch unixTimeUnit {
	case SecUnixUnixName:
		generate = func(v time.Time) int64 {
			return v.Unix()
		}
	case MilliUnixUnixName:
		generate = func(v time.Time) int64 {
			return v.UnixMilli()
		}
	case MicroUnixUnixName:
		generate = func(v time.Time) int64 {
			return v.UnixMicro()
		}
	case NanoUnixUnixName:
		generate = func(v time.Time) int64 {
			return v.UnixNano()
		}
	}

	return func(driver dbmsDriver, input []byte) (output []byte, err error) {
		anyVal, err := driver.DecodeValueByTypeName(inputType, input)
		if err != nil {
			return nil, fmt.Errorf("unable to decode raw value to \"%s\": %w", inputType, err)
		}
		timeVal, ok := anyVal.(time.Time)
		if !ok {
			return nil, fmt.Errorf("received unexpected value %+v: expected time.Time", anyVal)
		}
		return driver.EncodeValueByTypeName("int8", generate(timeVal), output)
	}
}

func IntToBool(driver dbmsDriver, input []byte) (output []byte, err error) {
	floatVal, err := cast.ToFloat64E(string(input))
	if err != nil {
		return nil, fmt.Errorf("error decoding raw value to int: %w", err)
	}
	boolVal, err := cast.ToBoolE(floatVal)
	if err != nil {
		return nil, fmt.Errorf("error casting int value to bool: %w", err)
	}
	return driver.EncodeValueByTypeName("bool", boolVal, output)
}

func BoolToInt(driver dbmsDriver, input []byte) (output []byte, err error) {
	boolVal, err := cast.ToBoolE(string(input))
	if err != nil {
		return nil, fmt.Errorf("error decoding raw value to bool: %w", err)
	}
	floatVal, err := cast.ToFloat64E(boolVal)
	if err != nil {
		return nil, fmt.Errorf("error casting bool value to int: %w", err)
	}
	return driver.EncodeValueByTypeName("int8", int64(floatVal), output)
}
