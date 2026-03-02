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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/constraints"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	generators2 "github.com/greenmaskio/greenmask/pkg/common/transformers/generators"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

const (
	ParameterNameKeepNull = "keep_null"
	ParameterNameColumn   = "column"
	ParameterNameValidate = "validate"
	ParameterNameEngine   = "engine"
	ParameterNameTruncate = "truncate"
	ParameterNameMinRatio = "min_ratio"
	ParameterNameMaxRatio = "max_ratio"

	EngineParameterValueRandom        = "random"
	EngineParameterValueDeterministic = "deterministic"
	//EngineParameterValueHash - deprecated, use deterministic instead
	EngineParameterValueHash = "hash"
)

const (
	Int2Length = 2
	Int4Length = 4
	Int8Length = 8
)

var pgGlobalTypeMap = pgtype.NewMap()

var (
	defaultKeepNullParameterDefinition = parameters.MustNewParameterDefinition(
		ParameterNameKeepNull,
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(models.ParamsValue("true"))

	defaultValidateParameterDefinition = parameters.MustNewParameterDefinition(
		ParameterNameValidate,
		"validate the value via driver decoding procedure",
	).SetDefaultValue(models.ParamsValue("true"))

	defaultEngineParameterDefinition = parameters.MustNewParameterDefinition(
		ParameterNameEngine,
		"The engine used for generating the values [random, deterministic]",
	).SetDefaultValue([]byte(EngineParameterValueRandom)).
		SetRawValueValidator(engineValidator)

	defaultMinRatioParameterDefinition = parameters.MustNewParameterDefinition(
		ParameterNameMinRatio,
		"min random percentage for noise",
	).SetDefaultValue(models.ParamsValue("0.05"))

	defaultMaxRatioParameterDefinition = parameters.MustNewParameterDefinition(
		ParameterNameMaxRatio,
		"max random percentage for noise",
	).SetDefaultValue(models.ParamsValue("0.2"))

	defaultFloatTypeSizeParameterDefinition = parameters.MustNewParameterDefinition(
		"type_size",
		"float size (4 or 8). It is used if greenmask can't detect it from column length",
	).SetRawValueValidator(func(ctx context.Context, p *parameters.ParameterDefinition, v models.ParamsValue) error {
		val, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			validationcollector.FromContext(ctx).Add(
				models.NewValidationWarning().
					AddMeta("ParameterValue", string(v)).
					SetError(err).
					SetSeverity(models.ValidationSeverityError).
					SetMsg("unable to parse int value"),
			)
		}
		switch int(val) {
		case float4Length, float8Length:
			return nil
		}
		validationcollector.FromContext(ctx).Add(
			models.NewValidationWarning().
				AddMeta("ParameterValue", string(v)).
				AddMeta("AllowedValues", []int{float4Length, float8Length}).
				SetSeverity(models.ValidationSeverityError).
				SetMsg("invalid int size"),
		)
		return models.ErrFatalValidationError
	}).SetDefaultValue(models.ParamsValue("4"))

	defaultIntTypeSizeParameterDefinition = parameters.MustNewParameterDefinition(
		"type_size",
		"int size (2, 4 or 8). It is used if greenmask can't detect it from column length",
	).SetDefaultValue(models.ParamsValue("4")).
		SetRawValueValidator(func(ctx context.Context, p *parameters.ParameterDefinition, v models.ParamsValue) error {
			val, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				validationcollector.FromContext(ctx).Add(
					models.NewValidationWarning().
						AddMeta("ParameterValue", string(v)).
						SetError(err).
						SetSeverity(models.ValidationSeverityError).
						SetMsg("unable to parse int value"),
				)
			}
			switch int(val) {
			case Int2Length, Int4Length, Int8Length:
				return nil
			}
			validationcollector.FromContext(ctx).Add(
				models.NewValidationWarning().
					AddMeta("ParameterValue", string(v)).
					AddMeta("AllowedValues", []int{Int2Length, Int4Length, Int8Length}).
					SetSeverity(models.ValidationSeverityError).
					SetMsg("invalid int size"),
			)
			return models.ErrFatalValidationError
		})

	truncateParts = []string{
		transformers.YearTruncateName,
		transformers.MonthTruncateName,
		transformers.DayTruncateName,
		transformers.HourTruncateName,
		transformers.MinuteTruncateName,
		transformers.SecondTruncateName,
		transformers.MillisecondTruncateName,
		transformers.MicrosecondTruncateName,
		transformers.NanosecondTruncateName,
	}

	defaultTruncateDateParameterDefinition = parameters.MustNewParameterDefinition(
		ParameterNameTruncate,
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetRawValueValidator(validateDateTruncationParameterValue)
)

func validateDateTruncationParameterValue(
	ctx context.Context,
	_ *parameters.ParameterDefinition,
	v models.ParamsValue,
) error {
	if string(v) == "" || slices.Contains(truncateParts, string(v)) {
		return nil
	}
	validationcollector.FromContext(ctx).Add(
		models.NewValidationWarning().
			SetSeverity(models.ValidationSeverityError).
			AddMeta("ParameterValue", string(v)).
			AddMeta("AllowedValues", truncateParts).
			SetMsg("wrong truncation part value"),
	)
	return models.ErrFatalValidationError
}

func engineValidator(ctx context.Context, p *parameters.ParameterDefinition, v models.ParamsValue) error {
	value := string(v)
	switch value {
	case EngineParameterValueRandom, EngineParameterValueDeterministic, EngineParameterValueHash:
		return nil
	default:
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetMsg("invalid engine value").
				AddMeta("ParameterValue", value).
				SetSeverity(models.ValidationSeverityError))
	}
	return nil
}

// TransformationFunc - a transformation function. It has the same signature as
// commonininterfaces.Transformer.Transform method.
type TransformationFunc func(_ context.Context, r interfaces.Recorder) error

// TransformWithKeepNull - wrapper that simplifies the logic of keep null parameter. You can set
// the keep_null logic on transformer initialization. Just provide the main transformation function
// and the columnIdx (the index of the column to be transformed).
func TransformWithKeepNull(tf TransformationFunc, columnIdx int) TransformationFunc {
	return func(ctx context.Context, r interfaces.Recorder) error {
		isNull, err := r.IsNullByColumnIdx(columnIdx)
		if err != nil {
			return fmt.Errorf("unable to scan column value: %w", err)
		}
		if isNull {
			// If is null and need to keep null - do not change a record.
			return nil
		}
		return tf(ctx, r)
	}
}

// panicParameterDoesNotExists - panic helper for case when parameter is not found in the map.
// It is used everywhere in get helpers below.
func panicParameterDoesNotExists(parameterName string) {
	panic(
		fmt.Errorf(`parameter "%s" is not found: %w`,
			parameterName,
			models.ErrCheckTransformerImplementation),
	)
}

// panicParameterDoesNotExists - returns the parameter value by scanning a value into variable.
// The type is provided via generic parameter.
func getParameterValueWithName[T any](
	ctx context.Context,
	parameters map[string]parameters.Parameterizer,
	parameterName string,
) (T, error) {
	parameter, ok := parameters[parameterName]
	if !ok {
		panicParameterDoesNotExists(parameterName)
	}
	var res T
	if err := parameter.Scan(&res); err != nil {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta(models.MetaKeyParameterName, parameterName).
				SetError(err).
				SetMsg("error scanning parameter"))
		return res, models.ErrFatalValidationError
	}
	return res, nil
}

// panicParameterDoesNotExists - returns the parameter value by scanning a value into variable.
// The type is provided via generic parameter.
func getParameterValueWithNameAndDefault[T any](
	ctx context.Context,
	parameters map[string]parameters.Parameterizer,
	parameterName string,
	defaultValue T,
) (T, error) {
	parameter, ok := parameters[parameterName]
	if !ok {
		panicParameterDoesNotExists(parameterName)
	}
	if commonutils.Must(parameter.IsEmpty()) {
		return defaultValue, nil
	}
	var res T
	if err := parameter.Scan(&res); err != nil {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta(models.MetaKeyParameterName, parameterName).
				SetError(err).
				SetMsg("error scanning parameter"))
		return res, models.ErrFatalValidationError
	}
	return res, nil
}

// getColumnParameterValueWithName - simplifies the logic of common column parameter.
// It gets the column name, get column definition.
func getColumnParameterValueWithName(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
	parameterName string,
) (string, *models.Column, error) {
	columnName, err := getParameterValueWithName[string](ctx, parameters, parameterName)
	if err != nil {
		return "", nil, err
	}
	c, err := tableDriver.GetColumnByName(columnName)
	if err != nil {
		validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
			SetSeverity(models.ValidationSeverityError).
			AddMeta(models.MetaKeyParameterName, parameterName).
			AddMeta(models.MetaKeyParameterValue, columnName).
			SetError(err).
			SetMsg("error getting column value"))
		return "", nil, models.ErrFatalValidationError
	}
	return columnName, c, nil
}

// getColumnParameterValue - get a column parameter value with name "column". It does the same
// as getColumnParameterValueWithName helper.
func getColumnParameterValue(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (string, *models.Column, error) {
	return getColumnParameterValueWithName(ctx, tableDriver, parameters, ParameterNameColumn)
}

func getColumnContainerParameter[T parameters.ColumnContainer](
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
	parameterName string,
) ([]T, map[int]string, error) {
	parameter, ok := parameters[parameterName]
	if !ok {
		panicParameterDoesNotExists(parameterName)
	}
	var res []T
	if err := parameter.Scan(&res); err != nil {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta(models.MetaKeyParameterName, parameterName).
				SetError(err).
				SetMsg("error scanning parameter"))
		return nil, nil, models.ErrFatalValidationError
	}
	columns := make(map[int]string)
	for idx := range res {
		if !res[idx].IsAffected() {
			continue
		}
		c, err := tableDriver.GetColumnByName(res[idx].ColumnName())
		if err != nil {
			validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta(models.MetaKeyParameterName, parameterName).
				AddMeta(models.MetaKeyParameterValue, res[idx].ColumnName()).
				AddMeta("ContainerIdx", idx).
				SetError(err).
				SetMsg("error getting column value"))
			return nil, nil, models.ErrFatalValidationError
		}
		columns[c.Idx] = c.Name
	}
	return res, columns, nil
}

func getGenerateEngine(ctx context.Context, engineName string, size int) (generators2.Generator, error) {
	switch engineName {
	case EngineParameterValueRandom:
		return getRandomBytesGen(size)
	case EngineParameterValueDeterministic, EngineParameterValueHash:
		salt := commonutils.SaltFromCtx(ctx)
		return generators2.GetHashBytesGen(salt, size)
	}
	return nil, fmt.Errorf("unknown engine %s", engineName)
}

func getRandomBytesGen(size int) (generators2.Generator, error) {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("generate random bytes sequence: %w", err)
	}
	seed := int64(binary.LittleEndian.Uint64(buf))
	return generators2.NewRandomBytes(seed, size), nil
}

//func getPgInterval(parameters map[string]parameters.Parameterizer, name string) (time.Duration, error) {
//	// TODO: It's not stable and does not fully support pgsyntax.
//	src, err := getParameterValueWithName[string](context.Background(), parameters, name)
//	if err != nil {
//		return 0, err
//	}
//
//	var dst pgtype.Interval
//	if err := dst.Scan(src); err != nil {
//		return 0, fmt.Errorf("scan interval: %w", err)
//	}
//	dur := (time.Duration(dst.Days) * time.Hour * 24) +
//		(time.Duration(dst.Months) * 30 * time.Hour * 24) +
//		(time.Duration(dst.Microseconds) * time.Millisecond)
//	return dur, nil
//}

type Duration struct {
	Years        int `json:"years,omitempty"`
	Months       int `json:"months,omitempty"`
	Days         int `json:"days,omitempty"`
	Weeks        int `json:"weeks,omitempty"`
	Hours        int `json:"hours,omitempty"`
	Minutes      int `json:"minutes,omitempty"`
	Seconds      int `json:"seconds,omitempty"`
	Milliseconds int `json:"milliseconds,omitempty"`
	Microseconds int `json:"microseconds,omitempty"`
	Nanoseconds  int `json:"nanoseconds,omitempty"`
}

func (d *Duration) ToDuration() time.Duration {
	return (time.Duration(d.Years) * 365 * 24 * time.Hour) +
		(time.Duration(d.Months) * 30 * 24 * time.Hour) +
		(time.Duration(d.Days) * 24 * time.Hour) +
		(time.Duration(d.Weeks) * 7 * 24 * time.Hour) +
		(time.Duration(d.Hours) * time.Hour) +
		(time.Duration(d.Minutes) * time.Minute) +
		(time.Duration(d.Seconds) * time.Second) +
		(time.Duration(d.Milliseconds) * time.Millisecond) +
		(time.Duration(d.Microseconds) * time.Microsecond) +
		(time.Duration(d.Nanoseconds) * time.Nanosecond)
}

func (d *Duration) IsZero() bool {
	return d.Years == 0 &&
		d.Months == 0 &&
		d.Days == 0 &&
		d.Weeks == 0 &&
		d.Hours == 0 &&
		d.Minutes == 0 &&
		d.Seconds == 0 &&
		d.Milliseconds == 0 &&
		d.Microseconds == 0 &&
		d.Nanoseconds == 0
}

func defaultRatioValidator(
	ctx context.Context,
	p *parameters.ParameterDefinition,
	raw models.ParamsValue,
) error {
	var v Duration
	if err := json.Unmarshal(raw, &v); err != nil {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta(models.MetaKeyParameterName, p.Name).
				SetError(err).
				SetMsg("error parsing parameter value"))
		return errors.Join(err, models.ErrFatalValidationError)
	}
	if v.IsZero() {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta(models.MetaKeyParameterName, p.Name).
				SetMsg("parameter value must not be zero"))
		return models.ErrFatalValidationError
	}
	return nil
}

func isInDynamicMode(parameters map[string]parameters.Parameterizer) bool {
	for _, p := range parameters {
		if p.IsDynamic() {
			return true
		}
	}
	return false
}

func isValueInLimits[T constraints.Ordered](requestedThreshold, minValue, maxValue T) bool {
	return requestedThreshold >= minValue && requestedThreshold <= maxValue
}

func getOptionalMinAndMaxThresholds[T any](
	minParameter, maxParameter parameters.Parameterizer,
) (*T, *T, error) {
	var minVal, maxVal *T
	if !commonutils.Must(minParameter.IsEmpty()) {
		minVal = new(T)
		if err := minParameter.Scan(minVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
	}

	if !commonutils.Must(minParameter.IsEmpty()) {
		maxVal = new(T)
		if err := maxParameter.Scan(maxVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
	}

	return minVal, maxVal, nil
}

func getFloatLimits(size int) (float64, float64, error) {
	switch size {
	case float4Length:
		return -math.MaxFloat32, math.MaxFloat32, nil
	case float8Length:
		return -math.MaxFloat64, math.MaxFloat64, nil
	}

	return 0, 0, fmt.Errorf("unsupported float size %d", size)
}

func getNoiseFloatMinAndMaxThresholds[T constraints.Ordered](
	size int,
	minParam parameters.Parameterizer,
	maxParam parameters.Parameterizer,
	limitGetter func(int) (T, T, error),
) (T, T, error) {
	var zero T
	var requestedMinValue, requestedMaxValue T
	var minRequested, maxRequested bool
	minLimit, maxLimit, err := limitGetter(size)
	if err != nil {
		return zero, zero, fmt.Errorf("get limits: %w", err)
	}

	if minParam.IsDynamic() {
		minRequested = true
		err = minParam.Scan(&requestedMinValue)
		if err != nil {
			return zero, zero, fmt.Errorf("scnan \"min\" param: %w", err)
		}
		if !isValueInLimits[T](requestedMinValue, minLimit, maxLimit) {
			return zero, zero, fmt.Errorf("is value in limits: %w", err)
		}
	}

	if maxParam.IsDynamic() {
		maxRequested = true
		err = maxParam.Scan(&requestedMaxValue)
		if err != nil {
			return zero, zero, fmt.Errorf(`unable to scan "max" dynamic param: %w`, err)
		}
		if !isValueInLimits[T](requestedMaxValue, minLimit, maxLimit) {
			return zero, zero, fmt.Errorf("is value in limits: %w", err)
		}
	}

	if minRequested {
		minLimit = requestedMinValue
	}
	if maxRequested {
		maxLimit = requestedMaxValue
	}

	return minLimit, maxLimit, nil
}

func getIntThresholds(size int) (int64, int64, error) {
	switch size {
	case Int2Length:
		return math.MinInt16, math.MaxInt16, nil
	case Int4Length:
		return math.MinInt32, math.MaxInt32, nil
	case Int8Length:
		return math.MinInt16, math.MaxInt16, nil
	}

	return 0, 0, fmt.Errorf("unsupported int size %d", size)
}

func scanIPNet(src []byte, dest *net.IPNet) error {
	return pgGlobalTypeMap.Scan(pgtype.InetOID, pgx.TextFormatCode, src, dest)
}

func scanMacAddr(src []byte, dest *net.HardwareAddr) error {
	return pgGlobalTypeMap.Scan(pgtype.MacaddrOID, pgx.TextFormatCode, src, dest)
}

func defaultColumnContainerUnmarshaler[T parameters.ColumnContainer](
	_ context.Context, _ *parameters.ParameterDefinition, data models.ParamsValue,
) (
	[]parameters.ColumnContainer, error,
) {
	var columns []T
	if err := json.Unmarshal(data, &columns); err != nil {
		return nil, fmt.Errorf("unmarshal columns parameter: %w", err)
	}
	res := make([]parameters.ColumnContainer, len(columns))
	for i, c := range columns {
		res[i] = c // ok because T is constrained to implement ColumnContainer
	}
	return res, nil
}
