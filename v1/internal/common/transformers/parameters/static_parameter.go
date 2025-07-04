package parameters

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"text/template"
	"time"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	gmtemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

var (
	errUnknownParsingCase            = errors.New("unknown parsing case: use Scan method instead")
	errDestCannotBeNil               = errors.New("dest cannot be nil")
	errLinkedParameterMustBeIsColumn = errors.New("linked parameter must be a column: check transformer implementation")
	errParameterIsNotFound           = errors.New("parameter is not found: check transformer implementation")
)

type StaticParameter struct {
	// definition - the parameter definition
	definition *ParameterDefinition
	// Driver - table driver
	driver commonininterfaces.TableDriver
	// linkedColumnParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	linkedColumnParameter *StaticParameter
	// Column - column of the table that was assigned in the parsing procedure according to provided Column name in
	// parameter value. In this case value has textual column name
	Column *models.Column
	// value - cached parsed value after Scan or Value
	value any
	// rawValue - original raw value received from config
	rawValue models.ParamsValue
}

func NewStaticParameter(def *ParameterDefinition, driver commonininterfaces.TableDriver) *StaticParameter {
	return &StaticParameter{
		definition: def,
		driver:     driver,
	}
}

func (sp *StaticParameter) IsEmpty() (bool, error) {
	if sp.rawValue != nil {
		return false, nil
	}
	if sp.definition.DefaultValue != nil {
		return false, nil
	}
	return true, nil
}

func (sp *StaticParameter) IsDynamic() bool {
	return false
}

func (sp *StaticParameter) GetDefinition() *ParameterDefinition {
	return sp.definition
}

func (sp *StaticParameter) linkColumnParameter(
	columnParams map[string]*StaticParameter,
) error {
	if sp.definition.LinkColumnParameter == "" {
		return nil
	}
	param, ok := columnParams[sp.definition.LinkColumnParameter]
	if !ok {
		return fmt.Errorf(
			"bug detected: check linked paramater the transformer definition '%s': %w",
			sp.definition.LinkColumnParameter,
			errParameterIsNotFound,
		)
	}
	sp.linkedColumnParameter = param
	if !sp.linkedColumnParameter.definition.IsColumn {
		return errLinkedParameterMustBeIsColumn
	}
	return nil
}

func (sp *StaticParameter) executeTemplate(vc *validationcollector.Collector) error {
	if len(sp.rawValue) == 0 || !sp.definition.SupportTemplate {
		return nil
	}
	tmpl, err := template.New("paramTemplate").
		Funcs(gmtemplate.FuncMap()).
		Parse(string(sp.rawValue))
	if err != nil {
		vc.Add(models.NewValidationWarning().
			SetSeverity(models.ValidationSeverityError).
			SetMsg("error parsing template in the parameter").
			AddMeta("Error", err.Error()).
			AddMeta("ParameterName", sp.definition.Name))
		return commonmodels.ErrFatalValidationError
	}
	buf := bytes.NewBuffer(nil)
	var columnName string
	if sp.linkedColumnParameter != nil {
		columnName = sp.linkedColumnParameter.Column.Name
	}
	spc := NewStaticParameterContext(sp.driver, columnName)
	if err = tmpl.Execute(buf, spc); err != nil {
		vc.Add(models.NewValidationWarning().
			SetSeverity(models.ValidationSeverityError).
			SetMsg("error executing template in the parameter").
			AddMeta("Error", err.Error()).
			AddMeta("ParameterValue", sp.rawValue),
		)
		return commonmodels.ErrFatalValidationError
	}
	sp.rawValue = buf.Bytes()
	return nil
}

func (sp *StaticParameter) validateValue(vc *validationcollector.Collector, rawValue models.ParamsValue) error {
	// We are comparing to nil because there can be empty string "" and it shouldn't be a nil pointer
	// and an empty value itself.
	if rawValue == nil {
		if sp.definition.Required && sp.definition.DefaultValue == nil {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("parameter is required").
				AddMeta("ParameterName", sp.definition.Name))
			return commonmodels.ErrFatalValidationError
		} else if sp.definition.DefaultValue != nil {
			sp.rawValue = slices.Clone(sp.definition.DefaultValue)
		}
	}

	if sp.definition.RawValueValidator != nil {
		err := sp.definition.RawValueValidator(vc, sp.definition, sp.rawValue)
		if err != nil {
			return fmt.Errorf("execute raw value validator: %w", err)
		}
		if vc.IsFatal() {
			return nil
		}
	}

	if sp.definition.IsColumn {
		columnName := string(rawValue)
		column, ok := sp.driver.GetColumnByName(columnName)
		if !ok {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("column does not exist").
				AddMeta("ColumnName", columnName).
				AddMeta("ParameterName", sp.definition.Name))
			return models.ErrFatalValidationError
		}
		sp.Column = column

		if sp.definition.ColumnProperties != nil &&
			len(sp.definition.ColumnProperties.AllowedTypes) > 0 &&
			!slices.Contains(sp.definition.ColumnProperties.AllowedTypes, sp.Column.TypeName) {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("unsupported column type").
				AddMeta("ColumnName", columnName).
				AddMeta("TypeName", sp.Column.TypeName).
				AddMeta("AllowedTypes", sp.definition.ColumnProperties.AllowedTypes),
			)
			return models.ErrFatalValidationError
		}
	}

	if sp.definition.AllowedValues != nil {
		if !slices.ContainsFunc(sp.definition.AllowedValues, func(allowedItem models.ParamsValue) bool {
			return slices.Compare(allowedItem, sp.rawValue) == 0
		}) {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("unknown parameter value").
				AddMeta("ParameterValue", string(sp.rawValue)).
				AddMeta("AllowedValues", parameterValuesToString(sp.definition.AllowedValues)))
			return models.ErrFatalValidationError
		}
	}
	return nil
}

func (sp *StaticParameter) Init(
	vc *validationcollector.Collector,
	columnParams map[string]*StaticParameter,
	rawValue models.ParamsValue,
) error {
	sp.rawValue = slices.Clone(rawValue)

	if err := sp.linkColumnParameter(columnParams); err != nil {
		return fmt.Errorf("link column parameter: %w", err)
	}

	if err := sp.executeTemplate(vc); err != nil {
		return fmt.Errorf("execute parameter template: %w", err)
	}

	if err := sp.validateValue(vc, sp.rawValue); err != nil {
		return fmt.Errorf("validate parameter value: %w", err)
	}

	return nil
}

func (sp *StaticParameter) Value() (any, error) {
	if sp.rawValue == nil {
		return nil, nil
	}

	if sp.value != nil {
		return sp.value, nil
	}

	res, err := getValue(sp.driver, sp.definition, sp.rawValue, sp.linkedColumnParameter)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sp *StaticParameter) RawValue() (models.ParamsValue, error) {
	return sp.rawValue, nil
}

func (sp *StaticParameter) Scan(dest any) error {
	if dest == nil {
		return errDestCannotBeNil
	}

	if sp.rawValue == nil {
		return nil
	}

	if sp.value != nil {
		// Assign value if it has been already parsed
		return utils.ScanPointer(sp.value, dest)
	}

	err := scanValue(sp.driver, sp.definition, sp.rawValue, sp.linkedColumnParameter, dest)
	if err != nil {
		return err
	}
	return nil

}

// TODO: Add unit tests
func getValue(
	driver commonininterfaces.DBMSDriver,
	definition *ParameterDefinition,
	rawValue models.ParamsValue,
	linkedColumnParameter *StaticParameter,
) (res any, err error) {
	switch {
	case definition.Unmarshaller != nil:
		res, err = definition.Unmarshaller(definition, driver, rawValue)
		if err != nil {
			return false, fmt.Errorf("execute custom unmarshaler: %w", err)
		}
	case linkedColumnParameter != nil:
		// Parsing dynamically - default value and type are unknown
		// TODO: Be careful - this may cause an error in Scan func if the the returning value is not a pointer
		res, err = driver.DecodeValueByTypeName(linkedColumnParameter.Column.TypeName, rawValue)
		if err != nil {
			return nil, fmt.Errorf("scan parameter via TableDriver: %w", err)
		}
	case definition.IsColumn:
		res = string(rawValue)
	default:
		return nil, errUnknownParsingCase
	}

	return res, nil
}

// TODO: Add unit tests
func scanValue(
	driver commonininterfaces.DBMSDriver,
	definition *ParameterDefinition,
	rawValue models.ParamsValue,
	linkedColumnParameter *StaticParameter,
	dest any,
) error {
	if dest == nil {
		return fmt.Errorf("dest cannot be nil")
	}

	if rawValue == nil {
		return nil
	}
	var res any

	switch {
	case definition.Unmarshaller != nil:
		value, err := definition.Unmarshaller(definition, driver, rawValue)
		if err != nil {
			return fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		return utils.ScanPointer(value, dest)
	case linkedColumnParameter != nil:
		if err := driver.ScanValueByTypeName(linkedColumnParameter.Column.TypeName, rawValue, dest); err != nil {
			return fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
		return nil
	}

	// If the parameter is a column, we can just assign the string value according to the rules below.
	switch dest.(type) {
	case string:
		val := string(rawValue)
		res = &val
		return utils.ScanPointer(res, dest)
	case *string:
		val := string(rawValue)
		res = &val
		return utils.ScanPointer(res, dest)
	case time.Duration:
		parsedDur, err := time.ParseDuration(string(rawValue))
		if err != nil {
			return fmt.Errorf("error parsing int64 value: %w", err)
		}
		res = &parsedDur
		return utils.ScanPointer(res, dest)
	case *time.Duration:
		parsedDur, err := time.ParseDuration(string(rawValue))
		if err != nil {
			return fmt.Errorf("error parsing int64 value: %w", err)
		}
		res = &parsedDur
		return utils.ScanPointer(res, dest)
	default:
		if err := json.Unmarshal(rawValue, dest); err != nil {
			return fmt.Errorf("unable to unmarshal value: %w", err)
		}
		return nil
	}
}

func parameterValuesToString(values []models.ParamsValue) []string {
	var res []string
	for _, val := range values {
		res = append(res, string(val))
	}

	return res
}
