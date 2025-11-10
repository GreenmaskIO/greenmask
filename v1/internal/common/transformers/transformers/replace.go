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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const TransformerNameReplace = "Replace"

var ReplaceTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameReplace,
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

	// keep_null parameter definition
	defaultKeepNullParameterDefinition,

	// validate parameter definition
	defaultValidateParameterDefinition,
)

type ReplaceTransformer struct {
	columnName          string
	columnIdx           int
	rawValue            *commonmodels.ColumnRawValue
	affectedColumns     map[int]string
	needValidate        bool
	columnOIDToValidate commonmodels.VirtualOID

	valueParam commonparameters.Parameterizer

	// transform function to be used for transforming the record.
	//
	// Depending on the value parameter type, it can be either static or dynamic.
	transform TransformationFunc
}

func NewReplaceTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, err
	}
	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, err
	}

	needValidate, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameValidate)
	if err != nil {
		return nil, err
	}

	valueParam := parameters["value"]
	var rawValue *commonmodels.ColumnRawValue

	if !valueParam.IsDynamic() {
		// Get value from parameter
		value, err := valueParam.RawValue()
		if err != nil {
			validationcollector.FromContext(ctx).
				Add(commonmodels.NewValidationWarning().
					SetSeverity(commonmodels.ValidationSeverityError).
					AddMeta(commonmodels.MetaKeyParameterName, valueParam.Name()).
					SetError(err).
					SetMsg("error getting parameter value"))
			return nil, commonmodels.ErrFatalValidationError
		}
		// Validate the value if requested
		if needValidate {
			_, err = tableDriver.DecodeValueByTypeOid(column.TypeOID, value)
			if err != nil {
				validationcollector.FromContext(ctx).
					Add(commonmodels.NewValidationWarning().
						SetSeverity(commonmodels.ValidationSeverityError).
						SetMsg("error validating parameter value").
						AddMeta(commonmodels.MetaKeyColumnTypeName, column.TypeName).
						AddMeta(commonmodels.MetaKeyParameterName, valueParam.Name()).
						AddMeta(commonmodels.MetaKeyParameterValue, string(value)).
						SetError(err))
				return nil, commonmodels.ErrFatalValidationError
			}
		}
		rawValue = commonmodels.NewColumnRawValue(value, false)
	}

	t := &ReplaceTransformer{
		columnName: columnName,
		affectedColumns: map[int]string{
			column.Idx: column.Name,
		},
		rawValue:            rawValue,
		columnIdx:           column.Idx,
		needValidate:        needValidate,
		columnOIDToValidate: column.TypeOID,
		valueParam:          valueParam,
	}

	// Set the transform function based on the value parameter type.
	t.transform = t.transformStatic
	if valueParam.IsDynamic() {
		t.transform = t.transformDynamic
	}
	if keepNull {
		t.transform = TransformWithKeepNull(t.transform, column.Idx)
	}

	return t, nil
}

func (t *ReplaceTransformer) getValueToReplace(
	driver commonininterfaces.TableDriver,
) (*commonmodels.ColumnRawValue, error) {
	// Check if the current dynamic value is empty (is null).
	isEmpty, err := t.valueParam.IsEmpty()
	if err != nil {
		return nil, fmt.Errorf("verify value is empty: %w", err)
	}
	if isEmpty {
		// if empty then create the toolkit.RawValue with null value
		// Note: we allow null values, though for dynamic values this may cause
		// 		 constraint violation due to unexpected null values that
		//       was got from the dynamic parameter.
		return commonmodels.NewColumnRawValue(nil, true), nil
	}
	// If not empty just get the raw value from the parameter
	v, err := t.valueParam.RawValue()
	if err != nil {
		return nil, fmt.Errorf(
			"get raw value from parameter \"%s\": %w",
			t.valueParam.Name(), err,
		)
	}
	if t.needValidate {
		_, err = driver.DecodeValueByTypeOid(t.columnOIDToValidate, v)
		if err != nil {
			return nil, fmt.Errorf("dynamic value validation error: %w", err)
		}
	}

	return commonmodels.NewColumnRawValue(v, false), nil
}

func (t *ReplaceTransformer) transformDynamic(
	_ context.Context,
	r commonininterfaces.Recorder,
) error {
	newValue, err := t.getValueToReplace(r.TableDriver())
	if err != nil {
		return fmt.Errorf("get value to replace: %w", err)
	}

	if err := r.SetRawColumnValueByIdx(t.columnIdx, newValue); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func (t *ReplaceTransformer) transformStatic(
	_ context.Context,
	r commonininterfaces.Recorder,
) error {
	if err := r.SetRawColumnValueByIdx(t.columnIdx, t.rawValue); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func (t *ReplaceTransformer) Transform(ctx context.Context, r commonininterfaces.Recorder) error {
	return t.transform(ctx, r)
}

func (t *ReplaceTransformer) Describe() string {
	return TransformerNameReplace
}

func (t *ReplaceTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *ReplaceTransformer) Init(_ context.Context) error {
	return nil
}

func (t *ReplaceTransformer) Done(_ context.Context) error {
	return nil
}
