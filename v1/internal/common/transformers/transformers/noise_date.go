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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	generators "github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators/transformers"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
)

const NoiseDateTransformerName = "NoiseDate"

var NoiseDateTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		NoiseDateTransformerName,
		"Add some random value (shift value) in the provided interval",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewNoiseDateTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz").
		SetSkipOnNull(true),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"min_ratio",
		"min random duration for noise. Dy default 5% of the max_ratio",
	).SetRequired(true).
		SetRawValueValidator(defaultRatioValidator).
		SetSupportTemplate(true),

	commonparameters.MustNewParameterDefinition(
		"max_ratio",
		"max random duration for noise",
	).SetRequired(true).
		SetRawValueValidator(defaultRatioValidator).
		SetSupportTemplate(true),

	commonparameters.MustNewParameterDefinition(
		"min",
		"min threshold date (and/or time) of value",
	).SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	commonparameters.MustNewParameterDefinition(
		"max",
		"max threshold date (and/or time) of value",
	).SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	defaultTruncateDateParameterDefinition,

	defaultEngineParameterDefinition,
)

type NoiseDateTransformer struct {
	t               *generators.NoiseTimestamp
	columnName      string
	columnIdx       int
	truncate        *string
	affectedColumns map[int]string
	maxParam        commonparameters.Parameterizer
	minParam        commonparameters.Parameterizer
	dynamicMode     bool
	transform       func(time.Time) (time.Time, error)
}

func NewNoiseDateTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
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

	truncate, err := getParameterValueWithName[string](ctx, parameters, ParameterNameTruncate)
	if err != nil {
		return nil, fmt.Errorf("error validating truncate value: %w", err)
	}

	var limiter *generators.NoiseTimestampLimiter
	if !dynamicMode {
		minValueThreshold, maxValueThreshold, err := getNoiseTimestampMinAndMaxThresholds(minParam, maxParam)
		if err != nil {
			return nil, fmt.Errorf("get min and max thresholds: %w", err)
		}
		limiter, err = generators.NewNoiseTimestampLimiter(minValueThreshold, maxValueThreshold)
		if err != nil {
			return nil, fmt.Errorf("unable to create timestamp limiter: %w", err)
		}
	}

	minRatio, err := getParameterValueWithName[Duration](ctx, parameters, "min_ratio")
	if err != nil {
		return nil, fmt.Errorf("get \"max_ratio\" param: %w", err)
	}
	maxRatio, err := getParameterValueWithName[Duration](ctx, parameters, "max_ratio")
	if err != nil {
		return nil, fmt.Errorf("get \"max_ratio\" param: %w", err)
	}

	t, err := generators.NewNoiseTimestamp(minRatio.ToDuration(), maxRatio.ToDuration(), truncate, limiter)
	if err != nil {
		return nil, fmt.Errorf("create noise timestamp transformer: %w", err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &NoiseDateTransformer{
		t:        t,
		truncate: &truncate,
		affectedColumns: map[int]string{
			column.Idx: column.Name,
		},
		columnIdx:  column.Idx,
		columnName: columnName,
		transform: func(v time.Time) (time.Time, error) {
			return t.Transform(nil, v)
		},
		maxParam:    maxParam,
		minParam:    minParam,
		dynamicMode: dynamicMode,
	}, nil
}

func (ndt *NoiseDateTransformer) GetAffectedColumns() map[int]string {
	return ndt.affectedColumns
}

func (ndt *NoiseDateTransformer) Init(context.Context) error {
	if ndt.dynamicMode {
		ndt.transform = ndt.dynamicTransform
	}
	return nil
}

func (ndt *NoiseDateTransformer) Done(context.Context) error {
	return nil
}

func (ndt *NoiseDateTransformer) dynamicTransform(v time.Time) (time.Time, error) {
	var (
		minVal *time.Time
		maxVal *time.Time
	)

	if err := ndt.minParam.Scan(minVal); err != nil {
		return time.Time{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	if err := ndt.maxParam.Scan(maxVal); err != nil {
		return time.Time{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := generators.NewNoiseTimestampLimiter(minVal, maxVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}

	res, err := ndt.t.Transform(limiter, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("error generating timestamp value: %w", err)
	}
	return res, nil
}

func (ndt *NoiseDateTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	var res time.Time
	isNull, err := r.ScanColumnValueByIdx(ndt.columnIdx, &res)
	if err != nil {
		return fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if isNull {
		return nil
	}

	res, err = ndt.transform(res)
	if err != nil {
		return fmt.Errorf("unable to transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(ndt.columnIdx, res); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func getNoiseTimestampMinAndMaxThresholds(
	minParameter, maxParameter commonparameters.Parameterizer,
) (*time.Time, *time.Time, error) {
	var minVal, maxVal *time.Time
	if !utils.Must(minParameter.IsEmpty()) {
		minVal = &time.Time{}
		if err := minParameter.Scan(&minVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
	}

	if !utils.Must(minParameter.IsEmpty()) {
		maxVal = &time.Time{}
		if err := maxParameter.Scan(&maxVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
	}

	return minVal, maxVal, nil
}
