package transformers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
)

type Unmarshaller func(parameter *Parameter, tableDriver *Driver, src []byte) (any, error)
type ValueValidator func(v any) error

const ColumnWithoutMaxLength = -1

// ColumnProperties - column-like parameter properties that would help to understand the affection on the consistency
type ColumnProperties struct {
	// Nullable - shows that transformer can produce NULL value for the column. Togather with Affected shows that
	// this parameter may generate null values and write it in this column. It only plays with Affected
	Nullable bool `json:"nullable,omitempty"`
	// Unique - shows that transformer guarantee that every transformer call the value will be unique. It only plays
	// with Affected
	Unique bool `json:"unique,omitempty"`
	// Unique - defines max length of the value. It only plays with Affected. Togather with Affected shows
	// that values will not exceed the length of the column. It only plays with Affected
	MaxLength int64 `json:"maxLength,omitempty"`
	// Affected - shows assigned column name will be affected after the transformation
	Affected bool `json:"affected,omitempty"`
	// AllowedColumnTypes - defines all the allowed column types in textual format. If not assigned (nil) then any
	// of the types is valid
	AllowedColumnTypes []string `json:"allowedColumnTypes,omitempty"`
}

func NewColumnProperties() *ColumnProperties {
	return &ColumnProperties{
		Nullable:  true,
		MaxLength: ColumnWithoutMaxLength,
	}
}

func (cp *ColumnProperties) SetNullable(v bool) *ColumnProperties {
	cp.Nullable = v
	return cp
}

func (cp *ColumnProperties) SetUnique(v bool) *ColumnProperties {
	cp.Unique = v
	return cp
}

func (cp *ColumnProperties) SetMaxLength(v int64) *ColumnProperties {
	cp.MaxLength = v
	return cp
}

func (cp *ColumnProperties) SetAffected(v bool) *ColumnProperties {
	cp.Affected = v
	return cp
}

// Parameter - wide parameter entity definition that contains properties that allows to check schema, find affection,
// cast variable using some features and so on. It may be defined and assigned ot the Definition of the transformer
// if transformer has any parameters
type Parameter struct {
	// Name - name of the parameter. Must be unique in the whole Transformer parameters slice
	Name string `json:"name,omitempty"`
	// Description - description of the parameter. Should contain the brief info about parameter
	Description string `json:"description,omitempty"`
	// Required - shows that parameter is required, and we expect we have to receive this value from config.
	// Event when DefaultValue is defined it will case error
	Required bool `json:"required,omitempty"`
	// IsColumn - shows is this parameter column related. If so ColumnProperties must be defined and assigned
	// otherwise it may cause an unhandled behaviour
	IsColumn bool `json:"isColumn,omitempty"`
	// LinkParameter - link with parameter with provided name. This is required if performing raw value encoding
	// depends on the provided column type and/or relies on the database Driver
	LinkParameter string `json:"linkParameter,omitempty"`
	// CastDbType - name of PostgreSQL type that would be used for Decoding raw value to the real go type. Is this
	// type does not exist will cause an error
	CastDbType string `json:"castDbType,omitempty"`
	// AllowedDbTypes - list of allowed column types that must be matched. Plays only when IsColumn.
	// Is empty or nil any type is allowed
	AllowedDbTypes []string `json:"allowedDbTypes"`
	// DefaultValue - default value of the parameter. Must be variable pointer and have the same type
	// as in ExpectedType
	DefaultValue any `json:"defaultValue,omitempty"`
	// ColumnProperties - detail info about expected column properties that may help to diagnose the table schema
	// and perform validation procedure
	ColumnProperties *ColumnProperties `json:"columnProperties,omitempty"`
	// Unmarshaller - unmarshal function for the parameter raw data []byte. Using by default json.Unmarshal function
	Unmarshaller Unmarshaller `json:"-"`
	// ValueValidator - value validator function that performs assertion and cause an error if it has violations
	ValueValidator ValueValidator `json:"-"`
	// LinkedParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	LinkedColumnParameter *Parameter `json:"-"`
	// Column - column of the table that was assigned in the parsing procedure according to provided column name in
	// parameter value. In this case value has textual column name
	Column *Column `json:"-"`
	// ExpectedType - expected type of the provided variable during scanning procedure. It must be pointer on the
	// variable
	ExpectedType any `json:"-"` // Must be pointer
	// value - parsed value of the parameter. It must be pointer on the variable
	value any
}

func MustNewParameter(name string, description string, expectedType any, defaultValue any) *Parameter {
	p, err := NewParameter(name, description, expectedType, defaultValue)
	if err != nil {
		panic(err)
	}
	return p
}

func NewParameter(name string, description string, expectedType any, defaultValue any) (*Parameter, error) {

	if expectedType == nil {
		return nil, fmt.Errorf("expected value cannot be nil")
	}
	// Check default type of ExpectedType and DefaultValue - they must be equal and assignable
	eValue := reflect.ValueOf(expectedType)
	if eValue.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("ExpectedType must be pointer")
	}
	eInd := reflect.Indirect(eValue)
	if !eInd.CanSet() {
		return nil, errors.New("ExpectedType is not settable")
	}

	value := expectedType

	if defaultValue != nil {
		dValue := reflect.ValueOf(defaultValue)
		if dValue.Kind() != reflect.Pointer {
			return nil, fmt.Errorf("DefaultValue must be pointer")
		}
		if eValue.Kind() == dValue.Kind() {
			dInd := reflect.Indirect(dValue)
			if eInd.Kind() != dInd.Kind() {
				return nil, errors.New("expectedValue and DefaultValue types are unequal")
			}
		} else {
			return nil, errors.New("expectedValue and DefaultValue types are unequal")
		}
		value = expectedType
	}

	return &Parameter{
		Name:         name,
		Description:  description,
		ExpectedType: expectedType,
		DefaultValue: defaultValue,
		value:        value,
	}, nil
}

// Parse - parse received params from the config using table definition. dest parameter must be pointer
func (p *Parameter) Parse(driver *Driver, params map[string][]byte, columnParams []*Parameter) (ValidationWarnings, error) {
	// Check allowed pgTypes exists
	for _, at := range p.AllowedDbTypes {
		_, ok := driver.TypeMap.TypeForName(at)
		if !ok {
			return nil, fmt.Errorf("AllowedDbType with name %s is not found", at)
		}
	}

	if params == nil {
		return nil, fmt.Errorf("paramas cannot be nil")
	}
	raw, ok := params[p.Name]
	if !ok {
		if p.Required {
			return nil, fmt.Errorf("paramater %s is required", p.Name)
		} else if p.DefaultValue != nil {
			p.value = p.DefaultValue
		} else if !p.Required {
			return nil, nil
		}
	}

	if p.LinkParameter != "" {
		idx := slices.IndexFunc(columnParams, func(parameter *Parameter) bool {
			return parameter.Name == p.LinkParameter
		})
		if idx == -1 {
			return nil, fmt.Errorf("link parameter %s does not exist", p.LinkParameter)
		}
		cp := columnParams[idx]
		if !cp.IsColumn {
			return nil, fmt.Errorf("cannot link with non column parameter")
		}
		p.LinkedColumnParameter = cp
	}

	if p.Unmarshaller != nil {
		// Perform custom unmarshalling
		value, err := p.Unmarshaller(p, driver, raw)
		if err != nil {
			return nil, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		p.value = value
	} else if p.CastDbType != "" {
		// Perform decoding via pgx driver
		switch p.value.(type) {
		case *time.Time:
			val, err := driver.DecodeByTypeName(p.CastDbType, raw)
			if err != nil {
				return nil, fmt.Errorf("unable to scan parameter via Driver")
			}
			valTime := val.(time.Time)
			p.value = &valTime
		default:
			if err := driver.ScanByTypeName(p.CastDbType, raw, p.value); err != nil {
				return nil, fmt.Errorf("unable to scan parameter via Driver")
			}
		}
	} else if p.LinkedColumnParameter != nil {

		switch p.value.(type) {
		case *time.Time:
			val, err := driver.DecodeByTypeOid(uint32(p.LinkedColumnParameter.Column.TypeOid), raw)
			if err != nil {
				return nil, fmt.Errorf("unable to scan parameter via Driver")
			}
			valTime := val.(time.Time)
			p.value = &valTime
		default:
			if err := driver.ScanByTypeOid(uint32(p.LinkedColumnParameter.Column.TypeOid), raw, p.value); err != nil {
				return nil, fmt.Errorf("unable to scan parameter via Driver")
			}
		}

		// Try to scan value using pgx driver and pgtype defined in the linked column
		if p.LinkedColumnParameter.Column == nil {
			return nil, fmt.Errorf("parameter is linked but column was not assigned")
		}

	} else if reflect.ValueOf(p.value).Kind() == reflect.String || (reflect.ValueOf(p.value).Kind() == reflect.Pointer &&
		reflect.Indirect(reflect.ValueOf(p.value)).Kind() == reflect.String) {
		// This is temporal solution for parsing string. Otherwise, it may cause an error in json.Unmarshall
		val := string(raw)
		p.value = &val
	} else {
		// Unmarshal as usual using json Umnarshaler
		if err := json.Unmarshal(raw, p.value); err != nil {
			return nil, fmt.Errorf("unable to unmarshal value: %w", err)
		}
	}

	if p.IsColumn {
		columnName, ok := p.value.(*string)
		if !ok {
			return nil, fmt.Errorf("unable to perform type assertion")
		}
		_, column, ok := driver.GetColumnByName(*columnName)
		if !ok {
			return ValidationWarnings{
				NewValidationWarning().
					SetMsg("column does not exist").
					AddMeta("columnName", *columnName).
					AddMeta("parameterName", p.Name),
			}, nil
		}
		pgType, _ := driver.TypeMap.TypeForOID(uint32(column.TypeOid))
		if len(p.AllowedDbTypes) > 0 && !slices.Contains(p.AllowedDbTypes, pgType.Name) {
			return ValidationWarnings{
				NewValidationWarning().
					SetMsg("unsupported column type").
					AddMeta("columnName", *columnName).
					AddMeta("columnType", pgType.Name).
					AddMeta("allowedDbTypes", p.AllowedDbTypes).
					AddMeta("parameterName", p.Name),
			}, nil
		}
		p.Column = column
	}

	if p.ValueValidator != nil {
		if err := p.ValueValidator(p.value); err != nil {
			return nil, fmt.Errorf("validation error: %w", err)
		}
	}
	return nil, nil
}

// Scan - scan parsed value into received pointer. Param src must be pointer
func (p *Parameter) Scan(dest any) error {
	return scanPointer(p.value, dest)
}

func (p *Parameter) SetLinkParameter(name string) *Parameter {
	if p.IsColumn {
		panic("cannot link column parameter with column parameter")
	}
	p.LinkParameter = name
	return p
}

func (p *Parameter) SetIsColumn(columnProperties *ColumnProperties) *Parameter {
	p.IsColumn = true
	p.ColumnProperties = columnProperties
	return p
}

func (p *Parameter) SetUnmarshaller(unmarshaller Unmarshaller) *Parameter {
	p.Unmarshaller = unmarshaller
	return p
}

func (p *Parameter) SetValueValidator(validator ValueValidator) *Parameter {
	p.ValueValidator = validator
	return p
}

// Value - returns parsed value that later might be cast via type assertion or so on
func (p *Parameter) Value() any {
	return p.value
}

func (p *Parameter) SelAllowedDbTypes(dbTypes []string) *Parameter {
	// Checking database types exists
	p.AllowedDbTypes = dbTypes
	return p
}

func (p *Parameter) SetRequired(v bool) *Parameter {
	// Checking database types exists
	p.Required = v
	return p
}
