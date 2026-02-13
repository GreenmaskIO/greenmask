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

package parameters

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"text/template"

	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/pkg/common/models"
	template2 "github.com/greenmaskio/greenmask/v1/pkg/common/transformers/template"
	"github.com/greenmaskio/greenmask/v1/pkg/common/utils"
	"github.com/greenmaskio/greenmask/v1/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
)

var (
	errCheckTransformerImplementation        = fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
	errReceivedNullValueFromDynamicParameter = fmt.Errorf("received NULL value from dynamic parameter and default value is not set")
)

type DynamicParameter struct {
	// DynamicValue - The dynamic value settings that received from config.
	DynamicValue *models.DynamicParamValue
	// definition - the parameter definition
	definition *ParameterDefinition
	// tableDriver - table driver.
	tableDriver interfaces.TableDriver
	// record - Record object for getting the value from record dynamically
	record interfaces.Recorder
	// tmpl - parsed and compiled template for casting the value from original to expected
	tmpl *template.Template
	// linkedColumnParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	linkedColumnParameter *StaticParameter
	// columnIdx - column number in the tuple
	columnIdx int
	buf       *bytes.Buffer
	column    *models.Column
	//defaultValueFromDynamicParamValue any
	//defaultValueFromDefinition        any

	hasDefaultValue     bool
	defaultValueScanned any
	defaultValueGot     any
	rawDefaultValue     models.ParamsValue
	tmplCtx             *DynamicParameterContext
	castToFunc          template2.TypeCastFunc
	// columnCanonicalTypeName - canonical type name of the column that is used for dynamic parameter.
	columnCanonicalTypeName      string
	columnCanonicalTypeClassName models.TypeClass
}

func NewDynamicParameter(def *ParameterDefinition, driver interfaces.TableDriver) *DynamicParameter {
	return &DynamicParameter{
		definition:  def,
		tableDriver: driver,
		buf:         bytes.NewBuffer(nil),
	}
}

func (dp *DynamicParameter) IsEmpty() (bool, error) {
	if dp.record == nil {
		return false, fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
	}

	rawValue, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return false, fmt.Errorf("erro getting raw column value: %w", err)
	}

	if !rawValue.IsNull {
		return false, nil
	}

	return !dp.hasDefaultValue, nil
}

func (dp *DynamicParameter) Name() string {
	return dp.definition.Name
}

func (dp *DynamicParameter) IsDynamic() bool {
	return true
}

func (dp *DynamicParameter) GetDefinition() *ParameterDefinition {
	return dp.definition
}

func (dp *DynamicParameter) SetRecord(r interfaces.Recorder) {
	dp.record = r
	dp.tmplCtx.setRecord(r)
}

func (dp *DynamicParameter) Init(
	ctx context.Context,
	columnParameters map[string]*StaticParameter,
	dynamicValue models.DynamicParamValue,
) error {
	dp.DynamicValue = &dynamicValue

	// Validate dynamic value
	if err := dp.validate(ctx, dp.DynamicValue); err != nil {
		return fmt.Errorf("validate value: %w", err)
	}

	// Determine default value
	dp.determineDefaultValue()

	// Render template
	if err := dp.renderTemplate(ctx); err != nil {
		return fmt.Errorf("render dynamic template: %w", err)
	}

	// Initialize dynamic parameter context
	// It gets column by name and sets it to the context
	if err := dp.initDynamicParameterContext(ctx); err != nil {
		return fmt.Errorf("init dynamic parameter context: %w", err)
	}

	// Initialize cast_to function
	if err := dp.initCastTo(ctx); err != nil {
		return fmt.Errorf("init cast to: %w", err)
	}

	// Initialize link column parameter
	if err := dp.initLinkColumnParameter(ctx, columnParameters); err != nil {
		return fmt.Errorf("initialize link column parameter: %w", err)
	}
	return nil
}

func (dp *DynamicParameter) validate(
	ctx context.Context,
	v *models.DynamicParamValue,
) error {
	if dp.definition.DynamicModeProperties == nil {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("parameter does not support dynamic mode"))
		return models.ErrFatalValidationError
	}

	if v == nil {
		panic("dynamicValue is nil: possibly bug")
	}

	if dp.DynamicValue.Column == "" {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("received empty \"column\" parameter").
				AddMeta("DynamicParameterSetting", "column"))
		return models.ErrFatalValidationError
	}

	if dp.definition.IsColumn {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("column parameter cannot work in dynamic mode"))
		return models.ErrFatalValidationError
	}
	return nil
}

func (dp *DynamicParameter) determineDefaultValue() {
	if dp.DynamicValue.DefaultValue != nil {
		dp.rawDefaultValue = slices.Clone(dp.DynamicValue.DefaultValue)
		dp.hasDefaultValue = true
	} else if dp.definition.DefaultValue != nil {
		dp.rawDefaultValue = slices.Clone(dp.definition.DefaultValue)
		dp.hasDefaultValue = true
	}
}

func (dp *DynamicParameter) renderTemplate(ctx context.Context) error {
	if dp.DynamicValue.Template == "" {
		return nil
	}
	var err error
	dp.tmpl, err = template.New("").
		Funcs(template2.FuncMap()).
		Parse(dp.DynamicValue.Template)
	if err != nil {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("unable to render cast template").
				SetError(err).
				AddMeta("DynamicParameterSetting", "cast_template"))
		return models.ErrFatalValidationError
	}
	return nil
}

func (dp *DynamicParameter) initDynamicParameterContext(ctx context.Context) error {
	column, err := dp.tableDriver.GetColumnByName(dp.DynamicValue.Column)
	if err != nil {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetError(err).
				SetMsg("error getting column by name").
				AddMeta("DynamicParameterSetting", "column").
				AddMeta("ColumnName", dp.definition.Name))
		return models.ErrFatalValidationError
	}
	dp.column = column
	dp.columnIdx = column.Idx
	dp.tmplCtx = NewDynamicParameterContext(column)
	return nil
}

func (dp *DynamicParameter) initCastTo(_ context.Context) error {
	if dp.DynamicValue.CastTo == "" {
		return nil
	}
	// TODO: Implement cast_to function execution for any type of DBMS.
	panic("IMPLEMENT ME")

	//var castFuncDef *gmtemplate.TypeCastDefinition
	//if dp.definition.LinkColumnParameter == "" {
	//	vc.Add(commonmodels.NewValidationWarning().
	//		SetSeverity(commonmodels.ValidationSeverityError).
	//		SetMsg("cast_to parameter is not supported for Non Linked transformer parameters").
	//		AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
	//		AddMeta("DynamicParameterSetting", "cast_to"))
	//	return
	//}
	//
	//castFuncDef, ok := gmtemplate.CastFunctionsMap[dp.DynamicValue.CastTo]
	//if !ok {
	//	vc.Add(commonmodels.NewValidationWarning().
	//		SetSeverity(commonmodels.ValidationSeverityError).
	//		SetMsg("unable to find cast_to function").
	//		AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
	//		AddMeta("DynamicParameterSetting", "cast_to"))
	//	return
	//}
	//dp.castToFunc = castFuncDef.Cast
}

func (dp *DynamicParameter) initLinkColumnParameter(
	ctx context.Context,
	columnParameters map[string]*StaticParameter,
) (err error) {
	if dp.definition.LinkColumnParameter == "" {
		return nil
	}
	param, ok := columnParameters[dp.definition.LinkColumnParameter]
	if !ok {
		panic(fmt.Sprintf(`parameter with name "%s" is not found`, dp.definition.LinkColumnParameter))
	}
	dp.linkedColumnParameter = param
	if !dp.linkedColumnParameter.definition.IsColumn {
		panic("bug: linked parameter must be column: check transformer implementation")
	}
	dp.tmplCtx.setLinkedColumn(dp.linkedColumnParameter.Column)

	dp.columnCanonicalTypeName, err = dp.tableDriver.GetCanonicalTypeName(dp.column.TypeName, dp.column.TypeOID)
	if err != nil {
		return fmt.Errorf("get input canonical type name: %w", err)
	}
	dp.columnCanonicalTypeClassName, err = dp.tableDriver.GetCanonicalTypeClassName(dp.column.TypeName, dp.column.TypeOID)
	if err != nil {
		return fmt.Errorf("get input canonical type class name: %w", err)
	}
	linkedParameterColumnType, err := dp.tableDriver.GetCanonicalTypeName(
		dp.linkedColumnParameter.Column.TypeName,
		dp.linkedColumnParameter.Column.TypeOID,
	)
	if err != nil {
		return fmt.Errorf("get output canonical type name: %w", err)
	}
	linkedParameterColumnTypeClass, err := dp.tableDriver.GetCanonicalTypeClassName(
		dp.linkedColumnParameter.Column.TypeName,
		dp.linkedColumnParameter.Column.TypeOID,
	)
	if err != nil {
		return fmt.Errorf("get output canonical type class name: %w", err)
	}

	if dp.tmpl != nil || dp.castToFunc != nil {
		// If we do have cast template or cast_to function, then we skip type compatibility check.
		return nil
	}

	// Check that column parameter has the same type with dynamic parameter value or at least dynamic parameter
	// column is compatible with type in the list. This logic is controversial since it might be unexpected
	// when dynamic param column has different though compatible types. Consider it
	if linkedParameterColumnType != dp.columnCanonicalTypeName &&
		linkedParameterColumnTypeClass != dp.columnCanonicalTypeClassName {
		vc := validationcollector.FromContext(ctx).
			WithMeta(map[string]any{
				"DynamicParameterAttribute":  "column",
				"DynamicParameterColumnType": dp.column.TypeName,
				"DynamicParameterColumnName": dp.column.Name,
				"LinkedParameterName":        dp.definition.LinkColumnParameter,
				"LinkedColumnName":           dp.linkedColumnParameter.Column.Name,
				"LinkedColumnType":           dp.linkedColumnParameter.Column.TypeName,
			})

		// If types are different, then we must check compatibility.
		properties := dp.linkedColumnParameter.definition.ColumnProperties
		if properties != nil && !properties.IsColumnTypeAllowed(dp.columnCanonicalTypeName) {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta("AllowedTypes", properties.AllowedTypes).
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				AddMeta("Reason", "type is not allowed").
				SetMsg("linked parameter and dynamic parameter column name has " +
					"different types and linked one is not allowed"))
			return models.ErrFatalValidationError
		}
		if properties != nil && !properties.IsColumnTypeClassAllowed(dp.columnCanonicalTypeClassName) {
			vc.Add(models.NewValidationWarning().
				AddMeta("AllowedTypeClasses", properties.AllowedTypeClasses).
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				AddMeta("Reason", "type class is not allowed").
				SetMsg("linked parameter and dynamic parameter column name has " +
					"different type classes and linked one is not allowed"))
			return models.ErrFatalValidationError
		}
		if properties != nil && properties.IsColumnTypeDenied(dp.columnCanonicalTypeName) {
			vc.Add(models.NewValidationWarning().
				AddMeta("DeniedTypes", properties.DeniedTypes).
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				AddMeta("Reason", "type is denied").
				SetMsg("linked parameter and dynamic parameter column name has " +
					"different types and linked one is not allowed"))
			return models.ErrFatalValidationError
		}
		if properties != nil && properties.IsColumnTypeClassDenied(dp.columnCanonicalTypeClassName) {
			vc.Add(models.NewValidationWarning().
				AddMeta("DeniedTypes", properties.DeniedTypeClasses).
				AddMeta("Reason", "type class is denied").
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				SetMsg("linked parameter and dynamic parameter column name has " +
					"different column class and linked one is not allowed"))
			return models.ErrFatalValidationError
		}
	}

	return nil
}

func (dp *DynamicParameter) Value() (value any, err error) {
	dp.buf.Reset()
	if dp.record == nil {
		return nil, errCheckTransformerImplementation
	}

	v, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("erro getting raw column value: %w", err)
	}

	if v.IsNull {
		if !dp.hasDefaultValue {
			return nil, fmt.Errorf("received NULL value from dynamic parameter")
		}
		if dp.defaultValueGot == nil {
			res, err := getValue(dp.tableDriver, dp.definition, dp.rawDefaultValue, dp.linkedColumnParameter)
			if err != nil {
				return nil, err
			}
			dp.defaultValueGot = res
		}
		return dp.defaultValueGot, nil
	}

	rawValue := v.Data

	if dp.tmpl != nil {
		// If a template is defined, we execute it to cast the value
		// and then use the result as a raw value.
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.tableDriver.Table().Schema).
				Str("FullTableName", dp.tableDriver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast template")

			return nil, fmt.Errorf("error executing cast template: %w", err)
		}
		rawValue = dp.buf.Bytes()
	} else if dp.castToFunc != nil {
		panic("IMPLEMENT ME")
		// TODO: Implement cast_to function execution for any type of DBMS.
		//rawValue, err = dp.castToFunc(dp.tableDriver, rawValue)
		//if err != nil {
		//	log.Debug().
		//		Err(err).
		//		Str("ParameterName", dp.definition.ID).
		//		Str("RawValue", string(v.Data)).
		//		Str("TableSchema", dp.tableDriver.Table().Schema).
		//		Str("FullTableName", dp.tableDriver.Table().ID).
		//		Str("Error", err.Error()).
		//		Msg("error executing cast_to function")
		//
		//	return nil, fmt.Errorf("error executing cast_to function: %w", err)
		//}
	}

	// Now get the raw value and decode it using the tableDriver or custom unmarshaller if defined.
	if dp.definition.DynamicModeProperties.Unmarshal != nil {
		res, err := dp.definition.DynamicModeProperties.Unmarshal(dp.tableDriver, dp.columnCanonicalTypeName, rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		return res, nil
	}

	// Decode the value using the tableDriver - it's default behaviour.
	res, err := dp.tableDriver.DecodeValueByColumnIdx(dp.columnIdx, rawValue)
	if err != nil {
		log.Debug().
			Err(err).
			Str("ParameterName", dp.definition.Name).
			Str("RawValue", string(v.Data)).
			Str("CastedValue", string(rawValue)).
			Str("TableSchema", dp.tableDriver.Table().Schema).
			Str("FullTableName", dp.tableDriver.Table().Name).
			Msg("error decoding casted value")

		return nil, fmt.Errorf("error scanning casted value: %w", err)
	}
	return res, nil

}

func (dp *DynamicParameter) RawValue() (models.ParamsValue, error) {
	if dp.record == nil {
		return nil, errCheckTransformerImplementation
	}

	rawValue, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, err
	}
	if rawValue.IsNull {
		if dp.hasDefaultValue {
			return dp.rawDefaultValue, nil
		}
		return nil, fmt.Errorf("received NULL value from dynamic parameter")
	}
	return rawValue.Data, nil
}

func (dp *DynamicParameter) Scan(dest any) error {
	dp.buf.Reset()
	if dp.record == nil {
		return errCheckTransformerImplementation
	}

	v, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return fmt.Errorf("error getting raw column value: %w", err)
	}

	if v.IsNull {
		if !dp.hasDefaultValue {
			return errReceivedNullValueFromDynamicParameter
		}

		if dp.defaultValueScanned == nil {
			err = scanValue(dp.tableDriver, dp.definition, dp.rawDefaultValue, dp.linkedColumnParameter, dest)
			if err != nil {
				return err
			}
			// TODO: You must copy scanned value since the dest is the pointer receiver otherwise it will cause
			// 	unexpected behaviour
			dp.defaultValueScanned = dest
			return nil
		}
		return utils.ScanPointer(dp.defaultValueScanned, dest)
	}

	rawValue := v.Data

	if dp.tmpl != nil {
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.tableDriver.Table().Schema).
				Str("FullTableName", dp.tableDriver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast template")

			return fmt.Errorf("error executing cast template: %w", err)
		}
		rawValue = dp.buf.Bytes()
	} else if dp.castToFunc != nil {
		panic("IMPLEMENT ME")
		// TODO: Implement cast_to function execution for any type of DBMS.
		//rawValue, err = dp.castToFunc(dp.tableDriver, rawValue)
		//if err != nil {
		//	log.Debug().
		//		Err(err).
		//		Str("ParameterName", dp.definition.ID).
		//		Str("RawValue", string(v.Data)).
		//		Str("TableSchema", dp.tableDriver.Table().Schema).
		//		Str("FullTableName", dp.tableDriver.Table().ID).
		//		Str("Error", err.Error()).
		//		Msg("error executing cast_to function")
		//
		//	return fmt.Errorf("error executing cast_to function: %w", err)
		//}
	}

	if dp.definition.DynamicModeProperties.Unmarshal != nil {
		value, err := dp.definition.DynamicModeProperties.Unmarshal(dp.tableDriver, dp.columnCanonicalTypeName, rawValue)
		if err != nil {
			return fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		return utils.ScanPointer(value, dest)
	}

	err = scanValue(dp.tableDriver, dp.definition, rawValue, dp.linkedColumnParameter, dest)
	if err != nil {
		log.Debug().
			Err(err).
			Str("ParameterName", dp.definition.Name).
			Str("RawValue", string(v.Data)).
			Str("CastedValue", string(rawValue)).
			Str("TableSchema", dp.tableDriver.Table().Schema).
			Str("FullTableName", dp.tableDriver.Table().Name).
			Msg("error decoding casted value")

		return fmt.Errorf("error scanning casted value: %w", err)
	}
	return nil
}
