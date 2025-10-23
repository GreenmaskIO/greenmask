// Copyright 2023 Greenmask
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
	"time"

	"github.com/greenmaskio/greenmask/internal/generators/transformers"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const RandomDateTransformerName = "RandomDate"

var RandomDateTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RandomDateTransformerName,
		"Generate date in the provided interval",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewTimestampTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"min",
		"min threshold date (and/or time) of value",
	).SetRequired(true).
		LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties(),
		),

	commonparameters.MustNewParameterDefinition(
		"max",
		"max threshold date (and/or time) of value",
	).SetRequired(true).
		LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	defaultTruncateDateParameterDefinition,

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type TimestampTransformer struct {
	*transformers.Timestamp
	columnName      string
	columnIdx       int
	keepNull        bool
	affectedColumns map[int]string

	maxParam    commonparameters.Parameterizer
	minParam    commonparameters.Parameterizer
	dynamicMode bool

	transform func([]byte) (time.Time, error)
}

type timestampMinMaxEncoder func(commonparameters.Parameterizer, commonparameters.Parameterizer) (
	time.Time, time.Time, error,
)

func NewTimestampTransformerBase(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
	encoder timestampMinMaxEncoder,
) (commonininterfaces.Transformer, error) {
	maxParam := parameters["max"]
	minParam := parameters["min"]

	dynamicMode := isInDynamicMode(parameters)

	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, fmt.Errorf("get \"keep_null\" param: %w", err)
	}

	truncate, err := getParameterValueWithName[string](ctx, parameters, ParameterNameTruncate)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	var minVal, maxVal time.Time
	var limiter *transformers.TimestampLimiter
	if !dynamicMode {
		minVal, maxVal, err = encoder(minParam, maxParam)

		if err != nil {
			return nil, fmt.Errorf("getmin and max values: %w", err)
		}
		limiter, err = transformers.NewTimestampLimiter(minVal, maxVal)
		if err != nil {
			return nil, fmt.Errorf("create timestamp limiter: %w", err)
		}
	}

	t, err := transformers.NewRandomTimestamp(truncate, limiter)
	if err != nil {
		return nil, err
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	return &TimestampTransformer{
		Timestamp:  t,
		keepNull:   keepNull,
		columnName: columnName,
		columnIdx:  column.Idx,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		minParam:    minParam,
		maxParam:    maxParam,
		dynamicMode: dynamicMode,
		transform: func(bytes []byte) (time.Time, error) {
			return t.Transform(nil, bytes)
		},
	}, nil
}

func NewTimestampTransformer(ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer) (commonininterfaces.Transformer, error) {
	return NewTimestampTransformerBase(ctx, tableDriver, parameters, getTimestampMinAndMaxThresholds)
}

func (rdt *TimestampTransformer) GetAffectedColumns() map[int]string {
	return rdt.affectedColumns
}

func (rdt *TimestampTransformer) Init(context.Context) error {
	if rdt.dynamicMode {
		rdt.transform = rdt.dynamicTransform
	}
	return nil
}

func (rdt *TimestampTransformer) Done(context.Context) error {
	return nil
}

func (rdt *TimestampTransformer) dynamicTransform(v []byte) (time.Time, error) {
	var minVal, maxVal time.Time
	err := rdt.minParam.Scan(&minVal)
	if err != nil {
		return time.Time{}, fmt.Errorf(`scan "min" param: %w`, err)
	}

	err = rdt.maxParam.Scan(&maxVal)
	if err != nil {
		return time.Time{}, fmt.Errorf(`scan "max" param: %w`, err)
	}

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

func (rdt *TimestampTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	valAny, err := r.GetRawColumnValueByIdx(rdt.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if valAny.IsNull && rdt.keepNull {
		return nil
	}
	res, err := rdt.transform(valAny.Data)
	if err != nil {
		return err
	}
	if err = r.SetColumnValueByIdx(rdt.columnIdx, res); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func getTimestampMinAndMaxThresholds(minParameter, maxParameter commonparameters.Parameterizer) (time.Time, time.Time, error) {
	var minVal, maxVal time.Time
	if err := minParameter.Scan(&minVal); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("scan \"min\" parameter: %w", err)
	}
	if err := maxParameter.Scan(&maxVal); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("scan \"max\" parameter: %w", err)
	}
	return minVal, maxVal, nil
}
