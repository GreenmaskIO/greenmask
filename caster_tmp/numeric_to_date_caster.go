package caster_tmp

import (
	"fmt"
	"slices"
	"time"

	"github.com/spf13/cast"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var NumericToDateCasterDefinition = &TypeCasterDefinitionV2{
	New:             NewNumericToDateCaster,
	InputTypeClass:  NumericClass,
	OutputTypeClass: DateClass,
}

type NumericToDateCaster struct {
	inputType  string
	outputType string
	decoder    func([]byte) (int64, error)
	encoder    func(int64) time.Time
}

func int64NanoToTime(v int64) time.Time {
	seconds := v / int64(time.Second)
	nano := v - seconds
	return time.Unix(seconds, nano)
}

func int64MicroToTime(v int64) time.Time {
	return time.UnixMicro(v)
}

func int64MilliToTime(v int64) time.Time {
	return time.UnixMilli(v)
}
func int64SecToTime(v int64) time.Time {
	return time.UnixMilli(v)
}

func NewNumericToDateCaster(driver *toolkit.Driver, inputType, outputType string, auto bool, params map[string]any) (TypeCasterV2, error) {

	if !toolkit.isTypeCompatible(nil, toolkit.NumericClass, inputType) {
		return nil, fmt.Errorf("unsupported input type \"%s\"", inputType)
	}

	if !toolkit.isTypeCompatible(nil, DateClass, outputType) {
		return nil, fmt.Errorf("unsupported output type \"%s\"", inputType)
	}

	var decoder func([]byte) (int64, error)

	if slices.Contains([]string{"numeric", "float4", "float8"}, inputType) {
		decoder = func(bytes []byte) (int64, error) {
			resFloat, err := cast.ToFloat64E(string(bytes))
			if err != nil {
				return 0, fmt.Errorf("error casting bytes to float64: %w", err)
			}
			return int64(resFloat), nil
		}
	}

	if slices.Contains([]string{"int2", "int4", "int8"}, inputType) {
		decoder = func(bytes []byte) (int64, error) {
			res, err := cast.ToInt64E(string(bytes))
			if err != nil {
				return 0, fmt.Errorf("error casting bytes to int64: %w", err)
			}
			return res, nil
		}
	}

	caster := &NumericToDateCaster{
		decoder:    decoder,
		inputType:  inputType,
		outputType: outputType,
	}

	if auto {
		caster.encoder = caster.makeDecisionAndEncode
	} else {
		unixTimeUnit, ok := params["unit"].(string)
		if !ok {
			return nil, fmt.Errorf("expected unix time unit such as (nano, micro, milli, sec) in non auto mode")
		}
		switch unixTimeUnit {
		case toolkit.secUnixUnixName:
			caster.encoder = int64SecToTime
		case toolkit.milliUnixUnixName:
			caster.encoder = int64MilliToTime
		case toolkit.microUnixUnixName:
			caster.encoder = int64MicroToTime
		case toolkit.nanoUnixUnixName:
			caster.encoder = int64NanoToTime
		default:
			return nil, fmt.Errorf("unknown unix time unit \"%s\"", unixTimeUnit)
		}
	}

	return caster, nil
}

func (nd *NumericToDateCaster) Cast(driver *toolkit.Driver, input []byte) (output []byte, err error) {
	res, err := nd.decoder(input) // NumericClass class -> int64
	if err != nil {
		return nil, fmt.Errorf("NumericToDateCaster error: error decoding value to int64: %w", err)
	}
	// int64 with specific Unix time unit (ms, ns etc.) to time.Time
	timeVal := nd.encoder(res)

	// time.Time to Data-like PostgreSQL representation
	return driver.EncodeValueByTypeName(nd.outputType, timeVal, output)
}

func (nd *NumericToDateCaster) makeDecisionAndEncode(val int64) time.Time {

	if val/1e9 > 0 {
		nd.encoder = int64NanoToTime
	} else if val/1e6 > 0 {
		nd.encoder = int64MicroToTime
	} else if val/1e3 > 0 {
		nd.encoder = int64MilliToTime
	} else {
		nd.encoder = int64SecToTime
	}
	return nd.encoder(val)
}
