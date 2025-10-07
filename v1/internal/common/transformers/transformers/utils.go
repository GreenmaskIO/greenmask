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
	"fmt"

	commonutils "github.com/greenmaskio/greenmask/internal/utils"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const (
	ParameterNameKeepNull = "keep_null"
	ParameterNameColumn   = "column"
	ParameterNameValidate = "validate"
	ParameterNameEngine   = "engine"

	EngineParameterValueRandom        = "random"
	EngineParameterValueDeterministic = "deterministic"
	//EngineParameterValueHash - deprecated, use deterministic instead
	EngineParameterValueHash = "hash"
)

var (
	ErrMaxRandomLengthMustBeGreaterThanZero = fmt.Errorf("max_random_length must be greater than 0")
)

var (
	defaultKeepNullParameterDefinition = commonparameters.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(commonmodels.ParamsValue("true"))

	defaultValidateParameterDefinition = commonparameters.MustNewParameterDefinition(
		ParameterNameValidate,
		"validate the value via driver decoding procedure",
	).SetDefaultValue(commonmodels.ParamsValue("true"))

	defaultEngineParameterDefinition = commonparameters.MustNewParameterDefinition(
		"engine",
		"The engine used for generating the values [random, deterministic]",
	).SetDefaultValue([]byte(EngineParameterValueRandom)).
		SetRawValueValidator(engineValidator)
)

func engineValidator(ctx context.Context, p *commonparameters.ParameterDefinition, v commonmodels.ParamsValue) error {
	value := string(v)
	switch value {
	case EngineParameterValueRandom, EngineParameterValueDeterministic, EngineParameterValueHash:
		return nil
	default:
		validationcollector.FromContext(ctx).
			Add(commonmodels.NewValidationWarning().
				SetMsg("invalid engine value").
				AddMeta("ParameterValue", value).
				SetSeverity(commonmodels.ValidationSeverityError))
	}
	return nil
}

// TransformationFunc - a transformation function. It has the same signature as
// commonininterfaces.Transformer.Transform method.
type TransformationFunc func(_ context.Context, r commonininterfaces.Recorder) error

// TransformWithKeepNull - wrapper that simplifies the logic of keep null parameter. You can set
// the keep_null logic on transformer initialization. Just provide the main transformation function
// and the columnIdx (the index of the column to be transformed).
func TransformWithKeepNull(tf TransformationFunc, columnIdx int) TransformationFunc {
	return func(ctx context.Context, r commonininterfaces.Recorder) error {
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
			commonmodels.ErrCheckTransformerImplementation),
	)
}

// panicParameterDoesNotExists - returns the parameter value by scanning a value into variable.
// The type is provided via generic parameter.
func getParameterValueWithName[T any](
	ctx context.Context,
	parameters map[string]commonparameters.Parameterizer,
	parameterName string,
) (T, error) {
	parameter, ok := parameters[parameterName]
	if !ok {
		panicParameterDoesNotExists(parameterName)
	}
	var res T
	if err := parameter.Scan(&res); err != nil {
		validationcollector.FromContext(ctx).
			Add(commonmodels.NewValidationWarning().
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta(commonmodels.MetaKeyParameterName, parameterName).
				SetError(err).
				SetMsg("error scanning parameter"))
		return res, commonmodels.ErrFatalValidationError
	}
	return res, nil
}

// getColumnParameterValueWithName - simplifies the logic of common column parameter.
// It gets the column name, get column definition.
func getColumnParameterValueWithName(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
	parameterName string,
) (string, *commonmodels.Column, error) {
	columnName, err := getParameterValueWithName[string](ctx, parameters, parameterName)
	if err != nil {
		return "", nil, err
	}
	c, err := tableDriver.GetColumnByName(columnName)
	if err != nil {
		validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta(commonmodels.MetaKeyParameterName, parameterName).
			AddMeta(commonmodels.MetaKeyParameterValue, columnName).
			SetError(err).
			SetMsg("error getting column value"))
		return "", nil, commonmodels.ErrFatalValidationError
	}
	return columnName, c, nil
}

// getColumnParameterValue - get a column parameter value with name "column". It does the same
// as getColumnParameterValueWithName helper.
func getColumnParameterValue(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (string, *commonmodels.Column, error) {
	return getColumnParameterValueWithName(ctx, tableDriver, parameters, ParameterNameColumn)
}

func getGenerateEngine(ctx context.Context, engineName string, size int) (generators.Generator, error) {
	switch engineName {
	case EngineParameterValueRandom:
		return getRandomBytesGen(size)
	case EngineParameterValueDeterministic, EngineParameterValueHash:
		salt := commonutils.SaltFromCtx(ctx)
		return generators.GetHashBytesGen(salt, size)
	}
	return nil, fmt.Errorf("unknown engine %s", engineName)
}

func getRandomBytesGen(size int) (generators.Generator, error) {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error generating random bytes sequence: %w", err)
	}
	seed := int64(binary.LittleEndian.Uint64(buf))
	return generators.NewRandomBytes(seed, size), nil
}
