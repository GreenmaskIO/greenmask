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

package transformers2

import (
	"context"
	"fmt"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const ReplaceTransformerName = "Replace"

var ReplaceTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		ReplaceTransformerName,
		"Replace column value to the provided",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewReplaceTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"value",
		"value to replace",
	).SetRequired(true).
		SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties(),
		),

	commonparameters.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(commonmodels.ParamsValue("true")),

	commonparameters.MustNewParameterDefinition(
		"validate",
		"validate the value via driver decoding procedure",
	).SetDefaultValue(commonmodels.ParamsValue("true")),
)

type transformWithoutContextFn func(r commonininterfaces.Recorder) error

type ReplaceTransformer struct {
	columnName          string
	columnIdx           int
	keepNull            bool
	rawValue            *commonmodels.ColumnRawValue
	affectedColumns     map[int]string
	validate            bool
	columnOIDToValidate commonmodels.VirtualOID

	columnParam   commonparameters.Parameterizer
	valueParam    commonparameters.Parameterizer
	validateParam commonparameters.Parameterizer
	keepNullParam commonparameters.Parameterizer

	// transform function to be used for transforming the record.
	//
	// Depending on the value parameter type, it can be either static or dynamic.
	transform transformWithoutContextFn
}

func NewReplaceTransformer(
	_ context.Context,
	vc *validationcollector.Collector,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	var columnName string
	var keepNull, validate bool

	columnParam := parameters["column"]
	if err := columnParam.Scan(&columnName); err != nil {
		return nil, fmt.Errorf(`scam "column" parameter: %w`, err)
	}

	c, ok := tableDriver.GetColumnByName(columnName)
	if !ok {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta(commonmodels.MetaKeyParameterName, columnParam.Name()).
			AddMeta(commonmodels.MetaKeyParameterValue, columnName))
		return nil, commonmodels.ErrFatalValidationError
	}
	affectedColumns := make(map[int]string)
	affectedColumns[c.Idx] = columnName

	validateParam := parameters["validate"]
	if err := validateParam.Scan(&validate); err != nil {
		return nil, fmt.Errorf(`scan "validate" parameter: %w`, err)
	}

	columnOIDToValidate := c.TypeOID
	valueParam := parameters["value"]
	var rawValue *commonmodels.ColumnRawValue
	if !valueParam.IsDynamic() {
		// Get value from parameter
		value, err := valueParam.RawValue()
		if err != nil {
			return nil, fmt.Errorf(
				"error getting raw value from parameter \"%s\": %w",
				valueParam.Name(), err,
			)
		}
		// Validate the value if requested
		if validate {
			_, err = tableDriver.DecodeValueByTypeOid(c.TypeOID, value)
			if err != nil {
				vc.Add(commonmodels.NewValidationWarning().
					SetSeverity(commonmodels.ValidationSeverityError).
					SetMsg("error validating parameter value").
					AddMeta(commonmodels.MetaKeyColumnTypeName, c.TypeName).
					AddMeta(commonmodels.MetaKeyParameterName, valueParam.Name()).
					AddMeta(commonmodels.MetaKeyParameterValue, string(value)).
					SetError(err))
				return nil, commonmodels.ErrFatalValidationError
			}
		}
		rawValue = commonmodels.NewColumnRawValue(value, false)
	}

	keepNullParam := parameters["keep_null"]
	if err := keepNullParam.Scan(&keepNull); err != nil {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta(commonmodels.MetaKeyColumnTypeName, keepNullParam.Name()).
			SetMsg("error scanning param").
			SetError(err),
		)
		return nil, commonmodels.ErrFatalValidationError
	}
	t := &ReplaceTransformer{
		columnName:          columnName,
		keepNull:            keepNull,
		affectedColumns:     affectedColumns,
		rawValue:            rawValue,
		columnIdx:           c.Idx,
		validate:            validate,
		columnOIDToValidate: columnOIDToValidate,

		columnParam:   columnParam,
		validateParam: validateParam,
		valueParam:    valueParam,
		keepNullParam: keepNullParam,
	}

	// Set the transform function based on the value parameter type.
	t.transform = t.transformStatic
	if valueParam.IsDynamic() {
		t.transform = t.transformDynamic
	}
	return t, nil
}

func (rt *ReplaceTransformer) GetAffectedColumns() map[int]string {
	return rt.affectedColumns
}

func (rt *ReplaceTransformer) Init(_ context.Context) error {
	return nil
}

func (rt *ReplaceTransformer) Done(_ context.Context) error {
	return nil
}

func (rt *ReplaceTransformer) getValueToReplace(
	driver commonininterfaces.TableDriver,
) (*commonmodels.ColumnRawValue, error) {
	// Check if the current dynamic value is empty (is null).
	isEmpty, err := rt.valueParam.IsEmpty()
	if err != nil {
		return nil, fmt.Errorf("check if value is empty: %w", err)
	}
	if isEmpty {
		// if empty then create the toolkit.RawValue with null value
		// Note: we allow null values, though for dynamic values this may cause
		// 		 constraint violation due to unexpected null values that
		//       was got from the dynamic parameter.
		return commonmodels.NewColumnRawValue(nil, true), nil
	}
	// If not empty just get the raw value from the parameter
	v, err := rt.valueParam.RawValue()
	if err != nil {
		return nil, fmt.Errorf(
			"get raw value from parameter \"%s\": %w",
			rt.valueParam.Name(), err,
		)
	}
	if rt.validate {
		_, err = driver.DecodeValueByTypeOid(rt.columnOIDToValidate, v)
		if err != nil {
			return nil, fmt.Errorf("dynamic value validation error: %w", err)
		}
	}

	return commonmodels.NewColumnRawValue(v, false), nil
}

func (rt *ReplaceTransformer) transformDynamic(
	r commonininterfaces.Recorder,
) error {
	newValue, err := rt.getValueToReplace(r.TableDriver())
	if err != nil {
		return fmt.Errorf("get value to replace: %w", err)
	}

	if err := r.SetRawColumnValueByIdx(rt.columnIdx, newValue); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func (rt *ReplaceTransformer) transformStatic(
	r commonininterfaces.Recorder,
) error {
	if err := r.SetRawColumnValueByIdx(rt.columnIdx, rt.rawValue); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func (rt *ReplaceTransformer) Transform(
	_ context.Context,
	r commonininterfaces.Recorder,
) error {
	isNull, err := r.IsNullByColumnIdx(rt.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan column value value: %w", err)
	}
	if isNull && rt.keepNull {
		// If is null and need to keep null - do not change a record.
		return nil
	}

	return rt.transform(r)
}

func init() {
	transformerutils.DefaultTransformerRegistry.MustRegister(ReplaceTransformerDefinition)
}
