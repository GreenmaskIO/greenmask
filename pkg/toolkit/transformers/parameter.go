package transformers

import (
	"encoding/json"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"
	"reflect"
	"slices"
	"time"

	"github.com/pkg/errors"
)

type Unmarshaller func(parameter *Parameter, tableDriver *Driver, src []byte) (any, error)
type ValueValidator func(v any) (ValidationWarnings, error)

const WithoutMaxLength = -1

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
	MaxLength int `json:"maxLength,omitempty"`
	// Affected - shows assigned column name will be affected after the transformation
	Affected bool `json:"affected,omitempty"`
	// AllowedColumnTypes - defines all the allowed column types in textual format. If not assigned (nil) then any
	// of the types is valid
	// TODO: AllowedColumnTypes cah a problem if we set int and our column is int2, then it cause an error though
	//		 it is workable case. How to solve subtype or type "aliases" references should be considered ASAP.
	//		 Also it has problem with custom type naming because it has schema name and type name. It might be better
	//		 to describe types with {{ schemaName }}.{{ typeName }}, but then we have to implement types classes
	//		 (such as textual, digits, etc.)
	AllowedColumnTypes []string `json:"allowedColumnTypes,omitempty"`
}

func NewColumnProperties() *ColumnProperties {
	return &ColumnProperties{
		Nullable:  true,
		MaxLength: WithoutMaxLength,
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

func (cp *ColumnProperties) SetMaxLength(v int) *ColumnProperties {
	cp.MaxLength = v
	return cp
}

func (cp *ColumnProperties) SetAllowedColumnTypes(v ...string) *ColumnProperties {
	cp.AllowedColumnTypes = v
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
	// DefaultValue - default value of the parameter. Must be variable pointer and have the same type
	// as in ExpectedType
	DefaultValue any `json:"defaultValue,omitempty"`
	// ColumnProperties - detail info about expected column properties that may help to diagnose the table schema
	// and perform validation procedure Plays only with IsColumn
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
	// dynamicParse - shows was the parameter value parsed with unset expectedType and defaultValue. In this case Scan
	// function is not available because returning value might be non Pointer. It might be fixed in the future releases
	dynamicParse bool
}

func MustNewParameter(name string, description string, expectedType any, defaultValue any) *Parameter {
	p, err := NewParameter(name, description, expectedType, defaultValue)
	if err != nil {
		panic(err)
	}
	return p
}

func NewParameter(name string, description string, expectedType any, defaultValue any) (*Parameter, error) {

	if expectedType != nil {
		eValue := reflect.ValueOf(expectedType)
		if eValue.Kind() != reflect.Pointer {
			return nil, fmt.Errorf("ExpectedType must be pointer")
		}
		eInd := reflect.Indirect(eValue)
		if !eInd.CanSet() {
			return nil, errors.New("ExpectedType is not settable")
		}

		// Check default type of ExpectedType and DefaultValue - they must be equal and assignable
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
		}
	} else if expectedType == nil && defaultValue != nil {
		return nil, errors.New("default value must be set togather with expectedType")
	}

	return &Parameter{
		Name:         name,
		Description:  description,
		ExpectedType: expectedType,
		DefaultValue: defaultValue,
	}, nil
}

// Parse - parse received params from the config using table definition. dest parameter must be pointer
func (p *Parameter) Parse(driver *Driver, params map[string]domains.ParamsValue, columnParams map[string]*Parameter) (ValidationWarnings, error) {
	p.value = nil
	// Check allowed pgTypes exists
	if p.ColumnProperties != nil {
		for _, at := range p.ColumnProperties.AllowedColumnTypes {
			_, ok := driver.TypeMap.TypeForName(at)
			if !ok {
				return nil, fmt.Errorf("AllowedDbType with name %s is not found", at)
			}
		}
	} else if p.CastDbType != "" {
		_, ok := driver.TypeMap.TypeForName(p.CastDbType)
		if !ok {
			return nil, fmt.Errorf("CastDbType with name %s is not found", p.CastDbType)
		}
	}

	raw, ok := params[p.Name]
	if !ok {
		if p.Required {
			return nil, fmt.Errorf("paramater %s is required", p.Name)
		} else if p.DefaultValue != nil {
			p.value = p.DefaultValue
			return nil, nil
		} else if !p.Required {
			return nil, nil
		}
	}

	if p.LinkParameter != "" {
		cp, ok := columnParams[p.LinkParameter]
		if !ok {
			return nil, fmt.Errorf("link parameter %s does not exist", p.LinkParameter)
		}
		if !cp.IsColumn {
			return nil, fmt.Errorf("cannot link with non column parameter")
		}
		p.LinkedColumnParameter = cp
	}

	if p.ExpectedType != nil {
		p.value = p.ExpectedType
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

			// Try to scan value using pgx driver and pgtype defined in the linked column
			if p.LinkedColumnParameter.Column == nil {
				return nil, fmt.Errorf("parameter is linked but column was not assigned")
			}

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

		} else if reflect.ValueOf(p.value).Kind() == reflect.String || (reflect.ValueOf(p.value).Kind() == reflect.Pointer &&
			reflect.Indirect(reflect.ValueOf(p.value)).Kind() == reflect.String) {
			// This is temporal solution for parsing string. Otherwise, it may cause an error in json.Unmarshall
			val := string(raw)
			p.value = &val
		} else {
			// Unmarshal as usual using json Unmarshaler
			if len(raw) != 0 {
				if err := json.Unmarshal(raw, p.value); err != nil {
					return nil, fmt.Errorf("unable to unmarshal value: %w", err)
				}
			}
		}
	} else if p.LinkedColumnParameter != nil {
		p.dynamicParse = true
		// Parsing dynamically - default value and type are unknown
		// TODO: Be careful - this may cause an error in Scan func if the the returning value is not a pointer
		val, err := driver.DecodeByTypeOid(uint32(p.LinkedColumnParameter.Column.TypeOid), raw)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver")
		}
		p.value = val
	} else if p.CastDbType != "" {
		val, err := driver.DecodeByTypeName(p.CastDbType, raw)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver")
		}
		p.value = val
		p.dynamicParse = true
	} else {
		panic("unknown parsing case")
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
					SetSeverity(ErrorValidationSeverity).
					SetMsg("column does not exist").
					AddMeta("ColumnName", *columnName).
					AddMeta("ParameterName", p.Name),
			}, nil
		}
		pgType, _ := driver.TypeMap.TypeForOID(uint32(column.TypeOid))
		// Check allowed types. If len(AllowedColumnTypes) == 0 then any type is allowed
		if p.ColumnProperties != nil &&
			len(p.ColumnProperties.AllowedColumnTypes) > 0 &&
			!slices.Contains(p.ColumnProperties.AllowedColumnTypes, pgType.Name) {
			return ValidationWarnings{
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("unsupported column type").
					AddMeta("ColumnName", *columnName).
					AddMeta("ColumnType", pgType.Name).
					AddMeta("AllowedDbTypes", p.ColumnProperties.AllowedColumnTypes).
					AddMeta("parameterName", p.Name),
			}, nil
		}
		p.Column = column
	}

	if p.ValueValidator != nil {
		w, err := p.ValueValidator(p.value)
		if err != nil {
			return nil, fmt.Errorf("validation error: %w", err)
		}
		if len(w) > 0 {
			return w, nil
		}
	}
	return nil, nil
}

// Scan - scan parsed value into received pointer. Param src must be pointer
func (p *Parameter) Scan(dest any) error {
	if p.dynamicParse {
		return errors.New("dynamically parsed parameters are unscannable")
	}
	if p.value == nil {
		return nil
	}
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

func (p *Parameter) SetRequired(v bool) *Parameter {
	// Checking database types exists
	p.Required = v
	return p
}

func (p *Parameter) SetCastDbType(v string) *Parameter {
	p.CastDbType = v
	return p
}

func (p *Parameter) Copy() *Parameter {
	cp := &(*p)
	cp.value = nil
	return cp
}
