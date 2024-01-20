package toolkit

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

func (sp *StaticParameter) IsDynamic() bool {
	return true
}

func (sp *StaticParameter) GetDefinition() *ParameterDefinition {
	return sp.definition
}

func (sp *StaticParameter) Init(columnParams map[string]*StaticParameter, rawValue ParamsValue) (ValidationWarnings, error) {

	var warnings ValidationWarnings

	sp.rawValue = slices.Clone(rawValue)

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
		warns, err := sp.definition.RawValueValidator(sp.definition, rawValue)
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

	if sp.definition.IsColumn {
		columnName := string(rawValue)
		sp.value = columnName
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

		if sp.definition.ColumnProperties != nil {

			if len(sp.definition.ColumnProperties.AllowedTypes) > 0 {

				if !IsTypeAllowedWithTypeMap(
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

	if sp.definition.Unmarshaller != nil {
		// Perform custom unmarshalling
		val, err := sp.definition.Unmarshaller(sp.definition, sp.driver, sp.rawValue)
		if err != nil {
			return false, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		sp.value = val
	} else if sp.linkedColumnParameter != nil {
		// Parsing dynamically - default value and type are unknown
		// TODO: Be careful - this may cause an error in Scan func if the the returning value is not a pointer
		val, err := sp.driver.DecodeValueByTypeOid(uint32(sp.linkedColumnParameter.Column.TypeOid), sp.rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
		sp.value = val
	} else if sp.definition.CastDbType != "" {
		val, err := sp.driver.DecodeValueByTypeName(sp.definition.CastDbType, sp.rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
		sp.value = val
	} else if sp.definition.IsColumn {
		sp.value = string(sp.rawValue)
	} else {
		return nil, errors.New("unknown parsing case: use Scan method instead")
	}

	return sp.value, nil
}

func (sp *StaticParameter) RawValue() (ParamsValue, error) {
	return sp.rawValue, nil
}

func (sp *StaticParameter) Scan(dest any) error {
	sp.value = nil
	if dest == nil {
		return fmt.Errorf("dest cannot be nil")
	}

	if sp.rawValue == nil {
		return nil
	}

	sp.value = dest
	if sp.definition.Unmarshaller != nil {
		// Perform custom unmarshalling
		value, err := sp.definition.Unmarshaller(sp.definition, sp.driver, sp.rawValue)
		if err != nil {
			return fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		sp.value = value
	} else if sp.definition.CastDbType != "" {
		// Perform decoding via pgx Driver
		switch sp.value.(type) {
		case *time.Time:
			val, err := sp.driver.DecodeValueByTypeName(sp.definition.CastDbType, sp.rawValue)
			if err != nil {
				return fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			sp.value = &valTime
		default:
			if err := sp.driver.ScanValueByTypeName(sp.definition.CastDbType, sp.rawValue, sp.value); err != nil {
				return fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
		}
	} else if sp.linkedColumnParameter != nil {

		// Try to scan value using pgx Driver and pgtype defined in the linked column
		if sp.linkedColumnParameter.Column == nil {
			return fmt.Errorf("parameter is linked but Column was not assigned")
		}

		switch sp.value.(type) {
		case *time.Time:
			val, err := sp.driver.DecodeValueByTypeOid(uint32(sp.linkedColumnParameter.Column.TypeOid), sp.rawValue)
			if err != nil {
				return fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			sp.value = &valTime
		default:
			if err := sp.driver.ScanValueByTypeOid(uint32(sp.linkedColumnParameter.Column.TypeOid), sp.rawValue, sp.value); err != nil {
				return fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
		}

	} else {

		switch sp.value.(type) {
		case string:
			val := string(sp.rawValue)
			sp.value = &val
		case *string:
			val := string(sp.rawValue)
			sp.value = &val
		case time.Duration:
			res, err := time.ParseDuration(string(sp.rawValue))
			if err != nil {
				return fmt.Errorf("error parsing int64 value: %w", err)
			}
			sp.value = &res
		case *time.Duration:
			res, err := time.ParseDuration(string(sp.rawValue))
			if err != nil {
				return fmt.Errorf("error parsing int64 value: %w", err)
			}
			sp.value = &res
		default:
			if err := json.Unmarshal(sp.rawValue, sp.value); err != nil {
				return fmt.Errorf("unable to unmarshal value: %w", err)
			}
		}

	}

	if sp.value == nil {
		// TODO: This is controversial logic - double check it
		return nil
	}
	return ScanPointer(sp.value, dest)
}
