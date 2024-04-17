package toolkit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"text/template"
	"time"
)

type StaticParameter struct {
	// definition - the parameter definition
	definition *ParameterDefinition
	// Driver - table driver
	driver *Driver
	// linkedColumnParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	linkedColumnParameter *StaticParameter
	// Column - column of the table that was assigned in the parsing procedure according to provided Column name in
	// parameter value. In this case value has textual column name
	Column *Column
	// value - cached parsed value after Scan or Value
	value any
	// rawValue - original raw value received from config
	rawValue ParamsValue
}

func NewStaticParameter(def *ParameterDefinition, driver *Driver) *StaticParameter {
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

func (sp *StaticParameter) Init(columnParams map[string]*StaticParameter, rawValue ParamsValue) (ValidationWarnings, error) {

	var warnings ValidationWarnings

	sp.rawValue = slices.Clone(rawValue)

	if sp.definition.GetFromGlobalEnvVariable != "" {
		sp.rawValue = []byte(os.Getenv(sp.definition.GetFromGlobalEnvVariable))
	}
	if rawValue != nil {
		sp.rawValue = slices.Clone(rawValue)
	}

	if sp.definition.LinkColumnParameter != "" {
		param, ok := columnParams[sp.definition.LinkColumnParameter]
		if !ok {
			panic(fmt.Sprintf(`parameter with name "%s" is not found`, sp.definition.LinkColumnParameter))
		}
		sp.linkedColumnParameter = param
		if !sp.linkedColumnParameter.definition.IsColumn {
			return nil, fmt.Errorf("linked parameter must be column: check transformer implementation")
		}
	}

	if len(sp.rawValue) > 0 && sp.definition.SupportTemplate {
		tmpl, err := template.New("paramTemplate").
			Funcs(FuncMap()).
			Parse(string(sp.rawValue))

		if err != nil {
			return ValidationWarnings{
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("error parsing template in the parameter").
						AddMeta("Error", err.Error()).
						AddMeta("ParameterName", sp.definition.Name),
				},
				nil
		}
		buf := bytes.NewBuffer(nil)
		spc := NewStaticParameterContext(sp.driver, sp.linkedColumnParameter.Column.Name)
		if err = tmpl.Execute(buf, spc); err != nil {
			return nil, fmt.Errorf("error executing template: %w", err)
		}
		sp.rawValue = buf.Bytes()
	}

	if rawValue == nil {
		if sp.definition.Required {
			return ValidationWarnings{
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("parameter is required").
						AddMeta("ParameterName", sp.definition.Name),
				},
				nil
		} else if sp.definition.DefaultValue != nil {
			sp.rawValue = sp.definition.DefaultValue
		}
	}

	if sp.definition.RawValueValidator != nil {
		warns, err := sp.definition.RawValueValidator(sp.definition, sp.rawValue)
		if err != nil {
			return nil, fmt.Errorf("error performing parameter raw value validation: %w", err)
		}
		for _, w := range warns {
			w.AddMeta("ParameterName", sp.definition.Name)
		}
		warnings = append(warnings, warns...)
		if warnings.IsFatal() {
			return warnings, nil
		}
	}

	if sp.definition.IsColumn {
		columnName := string(rawValue)
		_, column, ok := sp.driver.GetColumnByName(columnName)
		if !ok {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("column does not exist").
					AddMeta("ColumnName", columnName).
					AddMeta("ParameterName", sp.definition.Name),
			)
			return warnings, nil
		}
		sp.Column = column

		columnTypeName := sp.Column.TypeName
		columnTypeOid := sp.Column.TypeOid
		if sp.Column.OverriddenTypeName != "" {
			columnTypeName = sp.Column.OverriddenTypeName
			columnTypeOid = 0
		}

		if sp.definition.ColumnProperties != nil &&
			len(sp.definition.ColumnProperties.AllowedTypes) > 0 &&
			!IsTypeAllowedWithTypeMap(
				sp.driver,
				sp.definition.ColumnProperties.AllowedTypes,
				columnTypeName,
				columnTypeOid,
				true,
			) {
			warnings = append(warnings, NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				SetMsg("unsupported column type").
				AddMeta("ColumnName", columnName).
				AddMeta("TypeName", columnTypeName).
				AddMeta("AllowedTypes", sp.definition.ColumnProperties.AllowedTypes),
			)

			return warnings, nil
		}
	}

	if sp.definition.CastDbType != "" {
		_, ok := sp.driver.SharedTypeMap.TypeForName(sp.definition.CastDbType)
		if !ok {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					AddMeta("ParameterName", sp.definition.Name).
					AddMeta("CastDbType", sp.definition.CastDbType).
					AddMeta("TransformerAllowedTypes", sp.definition.ColumnProperties.AllowedTypes).
					SetMsg(`cannot perform parameter parsing: unknown type cast type: check transformer implementation or ensure your DB has this type`),
			)

			return warnings, nil
		}
	}
	return warnings, nil
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

func (sp *StaticParameter) RawValue() (ParamsValue, error) {
	return sp.rawValue, nil
}

func (sp *StaticParameter) Scan(dest any) error {

	if dest == nil {
		return fmt.Errorf("dest cannot be nil")
	}

	if sp.rawValue == nil {
		return nil
	}

	if sp.value != nil {
		// Assign value if it has been already parsed
		return ScanPointer(sp.value, dest)
	}

	err := scanValue(sp.driver, sp.definition, sp.rawValue, sp.linkedColumnParameter, dest)
	if err != nil {
		return err
	}
	return nil

}

// TODO: Add unit tests
func getValue(driver *Driver, definition *ParameterDefinition, rawValue ParamsValue, linkedColumnParameter *StaticParameter) (res any, err error) {

	if definition.Unmarshaller != nil {
		// Perform custom unmarshalling
		res, err = definition.Unmarshaller(definition, driver, rawValue)
		if err != nil {
			return false, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
	} else if linkedColumnParameter != nil {
		// Parsing dynamically - default value and type are unknown
		// TODO: Be careful - this may cause an error in Scan func if the the returning value is not a pointer
		res, err = driver.DecodeValueByTypeOid(uint32(linkedColumnParameter.Column.TypeOid), rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
	} else if definition.CastDbType != "" {
		res, err = driver.DecodeValueByTypeName(definition.CastDbType, rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
	} else if definition.IsColumn {
		res = string(rawValue)
	} else {
		return nil, errors.New("unknown parsing case: use Scan method instead")
	}

	return res, nil

}

// TODO: Add unit tests
func scanValue(driver *Driver, definition *ParameterDefinition, rawValue ParamsValue, linkedColumnParameter *StaticParameter, dest any) error {
	if dest == nil {
		return fmt.Errorf("dest cannot be nil")
	}

	if rawValue == nil {
		return nil
	}
	var res any

	if definition.Unmarshaller != nil {
		// Perform custom unmarshalling
		value, err := definition.Unmarshaller(definition, driver, rawValue)
		if err != nil {
			return fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		res = value
		return ScanPointer(value, dest)
	} else if definition.CastDbType != "" || linkedColumnParameter != nil {

		var typeOid uint32
		if linkedColumnParameter != nil {
			typeOid = uint32(linkedColumnParameter.Column.TypeOid)
		} else {
			t, ok := driver.GetTypeMap().TypeForName(definition.CastDbType)
			if !ok {
				return fmt.Errorf("unable to find \"cast_db_type\" called \"%s\"", definition.CastDbType)
			}
			typeOid = t.OID
		}

		// Perform decoding via pgx Driver
		switch dest.(type) {
		case *time.Time:
			val, err := driver.DecodeValueByTypeOid(typeOid, rawValue)
			if err != nil {
				return fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			res = &valTime
			return ScanPointer(res, dest)
		default:
			if err := driver.ScanValueByTypeOid(typeOid, rawValue, dest); err != nil {
				return fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			res = dest
			return nil
		}
	}

	switch dest.(type) {
	case string:
		val := string(rawValue)
		res = &val
		return ScanPointer(res, dest)
	case *string:
		val := string(rawValue)
		res = &val
		return ScanPointer(res, dest)
	case time.Duration:
		parsedDur, err := time.ParseDuration(string(rawValue))
		if err != nil {
			return fmt.Errorf("error parsing int64 value: %w", err)
		}
		res = &parsedDur
		return ScanPointer(res, dest)
	case *time.Duration:
		parsedDur, err := time.ParseDuration(string(rawValue))
		if err != nil {
			return fmt.Errorf("error parsing int64 value: %w", err)
		}
		res = &parsedDur
		return ScanPointer(res, dest)
	default:
		if err := json.Unmarshal(rawValue, dest); err != nil {
			return fmt.Errorf("unable to unmarshal value: %w", err)
		}
		res = &dest
		return nil
	}
}
