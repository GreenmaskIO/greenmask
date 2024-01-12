package parameters

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type StaticParameter struct {
	definition            *toolkit.ParameterDefinition
	driver                *toolkit.Driver
	linkedColumnParameter *StaticParameter
	rawValue              toolkit.ParamsValue
	column                *toolkit.Column
	value                 any
}

func NewStaticParameter(def *toolkit.ParameterDefinition, driver *toolkit.Driver) *StaticParameter {
	return &StaticParameter{
		definition: def,
		driver:     driver,
	}
}

func (p *StaticParameter) GetDefinition() *toolkit.ParameterDefinition {
	return p.definition
}

func (p *StaticParameter) Init(columnParams []*StaticParameter, rawValue toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {

	var warnings toolkit.ValidationWarnings

	p.rawValue = slices.Clone(rawValue)

	if rawValue == nil {
		if p.definition.Required {
			return toolkit.ValidationWarnings{
					toolkit.NewValidationWarning().
						SetSeverity(toolkit.ErrorValidationSeverity).
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
		idx := slices.IndexFunc(columnParams, func(param *StaticParameter) bool {
			return param.definition.Name == p.definition.LinkColumnParameter
		})
		if idx == -1 {
			panic(fmt.Sprintf(`parameter with name "%s" is not found`, p.definition.LinkColumnParameter))
		}
		p.linkedColumnParameter = columnParams[idx]
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
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					SetMsg("column does not exist").
					AddMeta("ColumnName", columnName).
					AddMeta("ParameterName", p.definition.Name),
			)
			return warnings, nil
		}
		p.column = column

		columnTypeName := p.column.TypeName
		if p.column.OverriddenTypeName != "" {
			columnTypeName = p.column.OverriddenTypeName
		}

		if p.definition.ColumnProperties != nil {

			if len(p.definition.ColumnProperties.AllowedTypes) > 0 {

				if !toolkit.IsTypeAllowed(p.definition.ColumnProperties.AllowedTypes, p.driver.CustomTypes, columnTypeName, true) {
					warnings = append(warnings, toolkit.NewValidationWarning().
						SetSeverity(toolkit.ErrorValidationSeverity).
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
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
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
		val, err := p.driver.DecodeValueByTypeOid(uint32(p.linkedColumnParameter.column.TypeOid), p.rawValue)
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

func (p *StaticParameter) RawValue() (toolkit.ParamsValue, error) {
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
		if p.linkedColumnParameter.column == nil {
			return false, fmt.Errorf("parameter is linked but column was not assigned")
		}

		switch p.value.(type) {
		case *time.Time:
			val, err := p.driver.DecodeValueByTypeOid(uint32(p.linkedColumnParameter.column.TypeOid), p.rawValue)
			if err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			p.value = &valTime
		default:
			if err := p.driver.ScanValueByTypeOid(uint32(p.linkedColumnParameter.column.TypeOid), p.rawValue, p.value); err != nil {
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
	return false, toolkit.ScanPointer(p.value, dest)
}
