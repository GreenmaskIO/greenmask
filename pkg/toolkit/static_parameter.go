package toolkit

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

type StaticParameter struct {
	definition            *ParameterDefinition
	driver                *Driver
	linkedColumnParameter *StaticParameter
	rawValue              ParamsValue
	Column                *Column
	value                 any
}

func NewStaticParameter(def *ParameterDefinition, driver *Driver) *StaticParameter {
	return &StaticParameter{
		definition: def,
		driver:     driver,
	}
}

func (p *StaticParameter) GetDefinition() *ParameterDefinition {
	return p.definition
}

func (p *StaticParameter) Init(columnParams map[string]*StaticParameter, rawValue ParamsValue) (ValidationWarnings, error) {

	var warnings ValidationWarnings

	p.rawValue = slices.Clone(rawValue)

	if rawValue == nil {
		if p.definition.Required {
			return ValidationWarnings{
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("parameter is required").
						AddMeta("ParameterName", p.definition.Name),
				},
				nil
		} else if p.definition.DefaultValue != nil {
			rawValue = p.definition.DefaultValue
		}
	}

	if p.definition.RawValueValidator != nil {
		warns, err := p.definition.RawValueValidator(p.definition, rawValue)
		if err != nil {
			return nil, fmt.Errorf("error performing parameter raw value validation: %w", err)
		}
		for _, w := range warns {
			w.AddMeta("ParameterName", p.definition.Name)
		}
		warnings = append(warnings, warns...)
		if warnings.IsFatal() {
			return warnings, nil
		}
	}

	if p.definition.LinkColumnParameter != "" {
		param, ok := columnParams[p.definition.LinkColumnParameter]
		if !ok {
			panic(fmt.Sprintf(`parameter with name "%s" is not found`, p.definition.LinkColumnParameter))
		}
		p.linkedColumnParameter = param
		if !p.linkedColumnParameter.definition.IsColumn {
			return nil, fmt.Errorf("linked parameter must be column: check transformer implementation")
		}
	}

	if p.definition.IsColumn {
		columnName := string(rawValue)
		p.value = columnName
		_, column, ok := p.driver.GetColumnByName(columnName)
		if !ok {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("column does not exist").
					AddMeta("ColumnName", columnName).
					AddMeta("ParameterName", p.definition.Name),
			)
			return warnings, nil
		}
		p.Column = column

		columnTypeName := p.Column.TypeName
		columnTypeOid := p.Column.TypeOid
		if p.Column.OverriddenTypeName != "" {
			columnTypeName = p.Column.OverriddenTypeName
			columnTypeOid = 0
		}

		if p.definition.ColumnProperties != nil {

			if len(p.definition.ColumnProperties.AllowedTypes) > 0 {

				if !IsTypeAllowedWithTypeMap(
					p.driver,
					p.definition.ColumnProperties.AllowedTypes,
					columnTypeName,
					columnTypeOid,
					true,
				) {
					warnings = append(warnings, NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("unsupported column type").
						AddMeta("ColumnName", columnName).
						AddMeta("TypeName", columnTypeName).
						AddMeta("AllowedTypes", p.definition.ColumnProperties.AllowedTypes),
					)

					return warnings, nil
				}
			}

		}
	}

	if p.definition.CastDbType != "" {
		_, ok := p.driver.SharedTypeMap.TypeForName(p.definition.CastDbType)
		if !ok {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					AddMeta("ParameterName", p.definition.Name).
					AddMeta("CastDbType", p.definition.CastDbType).
					AddMeta("TransformerAllowedTypes", p.definition.ColumnProperties.AllowedTypes).
					SetMsg(`cannot perform parameter parsing: unknown type cast type: check transformer implementation or ensure your DB has this type`),
			)

			return warnings, nil
		}
	}
	return warnings, nil
}

func (p *StaticParameter) Value() (any, error) {
	if p.rawValue == nil {
		return nil, nil
	}

	if p.definition.Unmarshaller != nil {
		// Perform custom unmarshalling
		val, err := p.definition.Unmarshaller(p.definition, p.driver, p.rawValue)
		if err != nil {
			return false, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		p.value = val
	} else if p.definition.LinkedColumnParameter != nil {
		// Parsing dynamically - default value and type are unknown
		// TODO: Be careful - this may cause an error in Scan func if the the returning value is not a pointer
		val, err := p.driver.DecodeValueByTypeOid(uint32(p.linkedColumnParameter.Column.TypeOid), p.rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
		p.value = val
	} else if p.definition.CastDbType != "" {
		val, err := p.driver.DecodeValueByTypeName(p.definition.CastDbType, p.rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
		p.value = val
	} else if p.definition.IsColumn {
		p.value = string(p.rawValue)
	} else {
		return nil, errors.New("unknown parsing case: use Scan method instead")
	}

	return p.value, nil
}

func (p *StaticParameter) RawValue() (ParamsValue, error) {
	return p.rawValue, nil
}

func (p *StaticParameter) Scan(dest any) (bool, error) {
	p.value = nil
	if dest == nil {
		return false, fmt.Errorf("dest cannot be nil")
	}

	if p.rawValue == nil {
		return true, nil
	}

	p.value = dest
	if p.definition.Unmarshaller != nil {
		// Perform custom unmarshalling
		value, err := p.definition.Unmarshaller(p.definition, p.driver, p.rawValue)
		if err != nil {
			return false, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		p.value = value
	} else if p.definition.CastDbType != "" {
		// Perform decoding via pgx Driver
		switch p.value.(type) {
		case *time.Time:
			val, err := p.driver.DecodeValueByTypeName(p.definition.CastDbType, p.rawValue)
			if err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			p.value = &valTime
		default:
			if err := p.driver.ScanValueByTypeName(p.definition.CastDbType, p.rawValue, p.value); err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
		}
	} else if p.linkedColumnParameter != nil {

		// Try to scan value using pgx Driver and pgtype defined in the linked column
		if p.linkedColumnParameter.Column == nil {
			return false, fmt.Errorf("parameter is linked but Column was not assigned")
		}

		switch p.value.(type) {
		case *time.Time:
			val, err := p.driver.DecodeValueByTypeOid(uint32(p.linkedColumnParameter.Column.TypeOid), p.rawValue)
			if err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			p.value = &valTime
		default:
			if err := p.driver.ScanValueByTypeOid(uint32(p.linkedColumnParameter.Column.TypeOid), p.rawValue, p.value); err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
		}

	} else {

		switch p.value.(type) {
		case string:
			val := string(p.rawValue)
			p.value = &val
		case *string:
			val := string(p.rawValue)
			p.value = &val
		case time.Duration:
			res, err := time.ParseDuration(string(p.rawValue))
			if err != nil {
				return false, fmt.Errorf("error parsing int64 value: %w", err)
			}
			p.value = &res
		case *time.Duration:
			res, err := time.ParseDuration(string(p.rawValue))
			if err != nil {
				return false, fmt.Errorf("error parsing int64 value: %w", err)
			}
			p.value = &res
		default:
			if err := json.Unmarshal(p.rawValue, p.value); err != nil {
				return false, fmt.Errorf("unable to unmarshal value: %w", err)
			}
		}

	}

	if p.value == nil {
		return false, nil
	}
	return false, ScanPointer(p.value, dest)
}
