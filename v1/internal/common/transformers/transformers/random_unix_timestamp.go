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

package transformers

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators/transformers"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
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

var UnixTimestampTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RandomUnixTimestampTransformerName,
		"Generate UnixTimestamp in the provided interval with unit",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewUnixTimestampTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("int2", "int4", "int8", "int", "smallint", "int", "smallint", "mediumint", "bigint"),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"min",
		"min threshold date (and/or time) of value",
	).SetRequired(true).
		SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("int2", "int4", "int8", "int", "smallint", "int", "smallint", "mediumint", "bigint"),
		),

	commonparameters.MustNewParameterDefinition(
		"max",
		"max threshold date (and/or time) of value",
	).SetRequired(true).
		SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("int2", "int4", "int8", "int", "smallint", "int", "smallint", "mediumint", "bigint"),
		),

	commonparameters.MustNewParameterDefinition(
		"min_unit",
		"min threshold date unit",
	).SetSupportTemplate(true).
		SetDefaultValue([]byte(secondsUnit)).
		SetRawValueValidator(validateDateUnitParameterValue),

	commonparameters.MustNewParameterDefinition(
		"max_unit",
		"max threshold date unit",
	).SetSupportTemplate(true).
		SetDefaultValue([]byte(secondsUnit)).
		SetRawValueValidator(validateDateUnitParameterValue),

	commonparameters.MustNewParameterDefinition(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetSupportTemplate(true).
		SetRawValueValidator(validateDateTruncationParameterValue),

	commonparameters.MustNewParameterDefinition(
		"unit",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetSupportTemplate(true).
		SetDefaultValue([]byte(secondsUnit)).
		SetRawValueValidator(validateDateUnitParameterValue),

	defaultIntTypeSizeParameterDefinition,

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type UnixTimestampTransformer struct {
	*TimestampTransformer
	unit    string
	minUnit string
	maxUnit string
}

func NewUnixTimestampTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {

	var unit, minUnit, maxUnit string
	var err error

	p := parameters["unit"]
	if err = p.Scan(&unit); err != nil {
		return nil, fmt.Errorf("scan \"unit\" param: %w", err)
	}

	p = parameters["min_unit"]
	if err = p.Scan(&minUnit); err != nil {
		return nil, fmt.Errorf("scan \"min_unit\" param: %w", err)
	}

	p = parameters["max_unit"]
	if err = p.Scan(&maxUnit); err != nil {
		return nil, fmt.Errorf("scan \"max_unit\" param: %w", err)
	}

	t, err := NewTimestampTransformerBase(
		ctx,
		tableDriver,
		parameters,
		getUnixTimestampMinAndMaxThresholds(minUnit, maxUnit),
	)
	if err != nil {
		return nil, err
	}

	return &UnixTimestampTransformer{
		TimestampTransformer: t.(*TimestampTransformer),
		unit:                 unit,
		minUnit:              minUnit,
		maxUnit:              maxUnit,
	}, nil

}

func (rdt *UnixTimestampTransformer) Init(context.Context) error {
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
		return time.Time{}, fmt.Errorf(`scan "min" param: %w`, err)
	}

	err = rdt.maxParam.Scan(&maxIntVal)
	if err != nil {
		return time.Time{}, fmt.Errorf(`scan "max" param: %w`, err)
	}

	minVal = getTimeByUnit(minIntVal, rdt.minUnit)
	maxVal = getTimeByUnit(maxIntVal, rdt.maxUnit)

	limiter, err := transformers.NewTimestampLimiter(minVal, maxVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("create limiter in dynamic mode: %w", err)
	}
	res, err := rdt.Timestamp.Transform(limiter, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("generate timestamp value: %w", err)
	}
	return res, nil
}

func (rdt *UnixTimestampTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	valAny, err := r.GetRawColumnValueByIdx(rdt.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if valAny.IsNull && rdt.keepNull {
		return nil
	}
	timeRes, err := rdt.transform(valAny.Data)
	if err != nil {
		return err
	}

	res := getUnixByUnit(timeRes, rdt.unit)

	if err = r.SetColumnValueByIdx(rdt.columnIdx, res); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func getTimeByUnit(v int64, unit string) time.Time {
	const (
		nanoPerSec = int64(1_000_000_000)
	)

	switch unit {
	case secondsUnit:
		return time.Unix(v, 0)
	case milliUnit:
		return time.UnixMilli(v)
	case microUnit:
		return time.UnixMilli(v)
	case nanoUnit:
		sec := v / nanoPerSec
		nano := v % nanoPerSec
		return time.Unix(sec, nano)
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

func validateDateUnitParameterValue(
	ctx context.Context,
	_ *commonparameters.ParameterDefinition,
	v commonmodels.ParamsValue,
) error {
	if slices.Contains(timestampUnitValues, string(v)) {
		return nil
	}

	validationcollector.FromContext(ctx).Add(
		commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta("ParameterValue", string(v)).
			AddMeta("AllowedValues", truncateParts).
			SetMsg("wrong timestamp unit value"))
	return commonmodels.ErrFatalValidationError
}

func getUnixTimestampMinAndMaxThresholds(minUnit, maxUnit string) timestampMinMaxEncoder {
	return func(minParameter, maxParameter commonparameters.Parameterizer) (time.Time, time.Time, error) {
		var minValInt, maxValInt int64
		if err := minParameter.Scan(&minValInt); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
		if err := maxParameter.Scan(&maxValInt); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}

		return getTimeByUnit(minValInt, minUnit), getTimeByUnit(maxValInt, maxUnit), nil
	}
}
