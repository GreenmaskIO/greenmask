package transformers

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	secondsUnit = "second"
	milliUnit   = "millisecond"
	microUnit   = "microsecond"
	nanoUnit    = "nanosecond"
)

const RandomUnixTimestampTransformerName = "RandomUnixTimestamp"

var timestampUnitValues = []string{
	secondsUnit, milliUnit, microUnit, nanoUnit,
}

var unixTimestampTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomUnixTimestampTransformerName,
		"Generate UnixTimestamp in the provided interval with unit",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewUnixTimestampTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("int2", "int4", "int8"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min threshold date (and/or time) of value",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("int2", "int4", "int8"),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max threshold date (and/or time) of value",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("int2", "int4", "int8"),
		),

	toolkit.MustNewParameterDefinition(
		"min_unit",
		"min threshold date unit",
	).SetSupportTemplate(true).
		SetLinkParameter("column").
		SetDefaultValue([]byte(secondsUnit)).
		SetRawValueValidator(validateDateUnitParameterValue),

	toolkit.MustNewParameterDefinition(
		"max_unit",
		"max threshold date unit",
	).SetSupportTemplate(true).
		SetLinkParameter("column").
		SetDefaultValue([]byte(secondsUnit)).
		SetRawValueValidator(validateDateUnitParameterValue),

	toolkit.MustNewParameterDefinition(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetSupportTemplate(true).
		SetRawValueValidator(validateDateTruncationParameterValue),

	toolkit.MustNewParameterDefinition(
		"unit",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetSupportTemplate(true).
		SetDefaultValue([]byte(secondsUnit)).
		SetRawValueValidator(validateDateUnitParameterValue),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type UnixTimestampTransformer struct {
	*TimestampTransformer
	unit    string
	minUnit string
	maxUnit string
}

func NewUnixTimestampTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var unit, minUnit, maxUnit string
	var err error

	p := parameters["unit"]
	if err = p.Scan(&unit); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"unit\" param: %w", err)
	}

	p = parameters["min_unit"]
	if err = p.Scan(&minUnit); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"min_unit\" param: %w", err)
	}

	p = parameters["max_unit"]
	if err = p.Scan(&maxUnit); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"max_unit\" param: %w", err)
	}

	t, warns, err := NewTimestampTransformerBase(ctx, driver, parameters, getUnixTimestampMinAndMaxThresholds(minUnit, maxUnit))

	if err != nil {
		return nil, warns, err
	}

	if warns.IsFatal() {
		return nil, warns, err
	}

	return &UnixTimestampTransformer{
		TimestampTransformer: t.(*TimestampTransformer),
		unit:                 unit,
		minUnit:              minUnit,
		maxUnit:              maxUnit,
	}, warns, nil

}

func (rdt *UnixTimestampTransformer) Init(ctx context.Context) error {
	if rdt.dynamicMode {
		rdt.transform = rdt.dynamicTransform
	}
	return nil
}

func (rdt *UnixTimestampTransformer) dynamicTransform(v []byte) (time.Time, error) {
	var minIntVal, maxIntVal int64
	var minVal, maxVal time.Time
	err := rdt.minParam.Scan(&minIntVal)
	if err != nil {
		return time.Time{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = rdt.maxParam.Scan(&maxIntVal)
	if err != nil {
		return time.Time{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	minVal = getTimeByUnit(minIntVal, rdt.minUnit)
	maxVal = getTimeByUnit(maxIntVal, rdt.maxUnit)

	limiter, err := transformers.NewTimestampLimiter(minVal, maxVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	res, err := rdt.Timestamp.Transform(limiter, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("error generating timestamp value: %w", err)
	}
	return res, nil
}

func (rdt *UnixTimestampTransformer) Transform(_ context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetRawColumnValueByIdx(rdt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rdt.keepNull {
		return r, nil
	}
	timeRes, err := rdt.transform(valAny.Data)
	if err != nil {
		return nil, err
	}

	res := getUnixByUnit(timeRes, rdt.unit)

	if err = r.SetColumnValueByIdx(rdt.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func getTimeByUnit(v int64, unit string) time.Time {
	switch unit {
	case secondsUnit:
		return time.Unix(v, 0)
	case milliUnit:
		return time.UnixMilli(v)
	case microUnit:
		return time.UnixMilli(v)
	case nanoUnit:
		seconds := v / 1e9
		nano := v % 999999999
		return time.Unix(seconds, nano)
	}
	panic(fmt.Sprintf("unknown unit: %s", unit))
}

func getUnixByUnit(v time.Time, unit string) int64 {
	switch unit {
	case secondsUnit:
		return v.Unix()
	case milliUnit:
		return v.UnixMilli()
	case microUnit:
		return v.UnixMicro()
	case nanoUnit:
		return v.UnixNano()
	}
	panic(fmt.Sprintf("unknown unit: %s", unit))
}

func validateDateUnitParameterValue(p *toolkit.ParameterDefinition, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {

	if !slices.Contains(timestampUnitValues, string(v)) {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("ParameterValue", string(v)).
				AddMeta("AllowedValues", truncateParts).
				SetMsg("wrong timestamp unit value"),
		}, nil
	}
	return nil, nil
}

func getUnixTimestampMinAndMaxThresholds(minUnit, maxUnit string) timestampMinMaxEncoder {

	return func(minParameter, maxParameter toolkit.Parameterizer) (time.Time, time.Time, error) {
		var minValInt, maxValInt int64
		if err := minParameter.Scan(&minValInt); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
		if err := maxParameter.Scan(&maxValInt); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}

		return getTimeByUnit(minValInt, minUnit), getTimeByUnit(maxValInt, minUnit), nil
	}
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(unixTimestampTransformerDefinition)
}
