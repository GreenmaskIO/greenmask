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

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// TODO: Ensure pqinterval.Duration returns duration in int64 for date and time

const NoiseDateTransformerName = "NoiseDate"

var NoiseDateTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		NoiseDateTransformerName,
		"Add some random value (shift value) in the provided interval",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewNoiseDateTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz").
		SetSkipOnNull(true),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min_ratio",
		"min random duration for noise. Dy default 5% of the max_ratio",
	).SetCastDbType("interval").
		SetSupportTemplate(true),

	toolkit.MustNewParameterDefinition(
		"max_ratio",
		"max random duration for noise",
	).SetRequired(true).
		SetCastDbType("interval").
		SetSupportTemplate(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min threshold date (and/or time) of value",
	).SetSupportTemplate(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max threshold date (and/or time) of value",
	).SetSupportTemplate(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	truncateDateParameterDefinition,

	engineParameterDefinition,
)

type NoiseDateTransformer struct {
	t               *transformers.NoiseTimestamp
	columnName      string
	columnIdx       int
	truncate        *string
	affectedColumns map[int]string

	columnParam   toolkit.Parameterizer
	maxRatioParam toolkit.Parameterizer
	minRatioParam toolkit.Parameterizer
	maxParam      toolkit.Parameterizer
	minParam      toolkit.Parameterizer
	engineParam   toolkit.Parameterizer
	truncateParam toolkit.Parameterizer

	transform func(time.Time) (time.Time, error)
}

type noiseTimestampMinMaxEncoder func(toolkit.Parameterizer, toolkit.Parameterizer) (*time.Time, *time.Time, error)

func NewNoiseDateTransformerBase(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer, encoder noiseTimestampMinMaxEncoder) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, engine, truncate string
	var minRatio, maxRatio pgtype.Interval
	var minValueThreshold, maxValueThreshold *time.Time
	var dynamicMode bool
	var warns toolkit.ValidationWarnings

	columnParam := parameters["column"]
	maxRatioParam := parameters["max_ratio"]
	minRatioParam := parameters["min_ratio"]
	maxParam := parameters["max"]
	minParam := parameters["min"]
	engineParam := parameters["engine"]
	truncateParam := parameters["truncate"]

	if err := engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if err := columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	if minParam.IsDynamic() || maxParam.IsDynamic() {
		dynamicMode = true
	}

	var limiter *transformers.NoiseTimestampLimiter
	var err error
	if !dynamicMode {
		minValueThreshold, maxValueThreshold, err = encoder(minParam, maxParam)

		if err != nil {
			return nil, nil, fmt.Errorf("error getting min and max values: %w", err)
		}
		// TODO: There might be another limiter
		limiter, err = transformers.NewNoiseTimestampLimiter(maxValueThreshold, maxValueThreshold)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create timestamp limiter: %w", err)
		}
	}

	if !dynamicMode {
		if err = minParam.Scan(&maxValueThreshold); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
		if err = maxParam.Scan(&minValueThreshold); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
	}

	if err = maxRatioParam.Scan(&maxRatio); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"max_ratio\" param: %w", err)
	}

	empty, _ := minRatioParam.IsEmpty()
	minRatioIsProvided := !empty
	if minRatioIsProvided {
		if err = minRatioParam.Scan(&minRatio); err != nil {
			return nil, nil, fmt.Errorf("unable to scan \"min_ratio\" param: %w", err)
		}
	}

	warn := validateIntervalValue(maxRatio)
	if warn != nil {
		warn.AddMeta("ParameterName", "max_ratio")
		warns = append(warns, warn)
	}

	maxRatioDuration := (time.Duration(maxRatio.Days) * time.Hour * 24) +
		(time.Duration(maxRatio.Months) * 30 * time.Hour * 24) +
		(time.Duration(maxRatio.Microseconds) * time.Millisecond)

	// By default min ration is 0.05% of max_ratio
	minRatioDuration := time.Duration(float64(maxRatioDuration) * 0.05)

	if minRatioIsProvided {
		if err = minRatioParam.Scan(&minRatio); err != nil {
			return nil, nil, fmt.Errorf("unable to scan \"max_ratio\" param: %w", err)
		}
		warn = validateIntervalValue(minRatio)
		if warn != nil {
			warn.AddMeta("ParameterName", "min_ratio")
			warns = append(warns, warn)
		}
		if warns.IsFatal() {
			return nil, warns, nil
		}

		minRatioDuration = (time.Duration(minRatio.Days) * time.Hour * 24) +
			(time.Duration(minRatio.Months) * 30 * time.Hour * 24) +
			(time.Duration(minRatio.Microseconds) * time.Millisecond)
	}

	if err = truncateParam.Scan(&truncate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "truncate" param: %w`, err)
	}

	t, err := transformers.NewNoiseTimestamp(minRatioDuration, maxRatioDuration, truncate, limiter)
	if err != nil {
		return nil, nil, err
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &NoiseDateTransformer{
		t:               t,
		columnName:      columnName,
		truncate:        &truncate,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
		transform: func(v time.Time) (time.Time, error) {
			return t.Transform(nil, v)
		},

		columnParam:   columnParam,
		maxRatioParam: maxRatioParam,
		minRatioParam: minRatioParam,
		maxParam:      maxParam,
		minParam:      minParam,
		engineParam:   engineParam,
		truncateParam: truncateParam,
	}, nil, nil
}

func NewNoiseDateTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	return NewNoiseDateTransformerBase(ctx, driver, parameters, getNoiseTimestampMinAndMaxThresholds)
}

func (ndt *NoiseDateTransformer) GetAffectedColumns() map[int]string {
	return ndt.affectedColumns
}

func (ndt *NoiseDateTransformer) Init(ctx context.Context) error {
	if ndt.columnParam.IsDynamic() {
		ndt.transform = ndt.dynamicTransform
	}
	return nil
}

func (ndt *NoiseDateTransformer) Done(ctx context.Context) error {
	return nil
}

func (ndt *NoiseDateTransformer) dynamicTransform(v time.Time) (time.Time, error) {
	minVal := &time.Time{}
	maxVal := &time.Time{}

	empty, err := ndt.minParam.IsEmpty()
	if err != nil {
		return time.Time{}, fmt.Errorf(`unable to check "min" param: %w`, err)
	}
	if !empty {
		if err = ndt.minParam.Scan(minVal); err != nil {
			return time.Time{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
		}
	}

	empty, err = ndt.maxParam.IsEmpty()
	if err != nil {
		return time.Time{}, fmt.Errorf(`unable to check "max" param: %w`, err)

	}
	if !empty {
		if err = ndt.maxParam.Scan(maxVal); err != nil {
			return time.Time{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
		}

	}

	limiter, err := transformers.NewNoiseTimestampLimiter(minVal, maxVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	res, err := ndt.t.Transform(limiter, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("error generating timestamp value: %w", err)
	}
	return res, nil
}

func (ndt *NoiseDateTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	var res time.Time
	isNull, err := r.ScanColumnValueByIdx(ndt.columnIdx, &res)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if isNull {
		return r, nil
	}

	res, err = ndt.transform(res)
	if err != nil {
		return nil, fmt.Errorf("unable to transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(ndt.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func validateIntervalValue(v pgtype.Interval) *toolkit.ValidationWarning {
	if v.Months == 0 && v.Days == 0 && v.Microseconds == 0 {
		return toolkit.NewValidationWarning().
			SetMsg(`error parsing ratio parameter or received 0 value`).
			SetSeverity(toolkit.ErrorValidationSeverity)

	}
	return nil
}

func getNoiseTimestampMinAndMaxThresholds(minParameter, maxParameter toolkit.Parameterizer) (*time.Time, *time.Time, error) {
	var minVal, maxVal *time.Time
	if empty, _ := minParameter.IsEmpty(); !empty {
		minVal = &time.Time{}
		if err := minParameter.Scan(&minVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
	}

	if empty, _ := minParameter.IsEmpty(); !empty {
		maxVal = &time.Time{}
		if err := maxParameter.Scan(&maxVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
	}

	return minVal, maxVal, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseDateTransformerDefinition)
}
