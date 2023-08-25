package transformers

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
)

type Unmarshaller func(parameter *Parameter, tableDriver *Driver, src []byte) (any, error)
type Validator func(v any) error

type Parameter struct {
	Name               string
	Description        string
	Required           bool
	IsColumn           bool
	AllowedColumnTypes []string
	Unmarshaller       Unmarshaller
	Validator          Validator
	ColumnParameter    *Parameter
	Column             *Column
	CastPgType         string
	ExpectedType       any // Must be pointer
	DefaultValue       any // Must be pointer
	value              any // Must be pointer
}

func MustNewParameter(name string, description string, expectedType any, defaultValue any,
	unmarshaller Unmarshaller, validator Validator,
) *Parameter {
	p, err := NewParameter(name, description, expectedType, defaultValue, unmarshaller, validator)
	if err != nil {
		panic(err)
	}
	return p
}

func NewParameter(name string, description string, expectedType any, defaultValue any,
	unmarshaller Unmarshaller, validator Validator,
) (*Parameter, error) {

	if expectedType == nil {
		return nil, fmt.Errorf("expected value cannot be nil")
	}
	// Check default type of ExpectedType and DefaultValue - they must be equal and assignable
	eValue := reflect.ValueOf(expectedType)
	if eValue.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("ExpectedType must be pointer")
	}
	eInd := reflect.Indirect(eValue)
	if eInd.CanSet() {
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
		Unmarshaller: unmarshaller,
		Validator:    validator,
		value:        value,
	}, nil
}

// Parse - parse received params from the config using table definition. dest parameter must be pointer
func (p *Parameter) Parse(tableDriver *Driver, params map[string][]byte) error {
	if params == nil {
		return fmt.Errorf("paramas cannot be nil")
	}
	raw, ok := params[p.Name]
	if !ok && p.Required {
		return fmt.Errorf("paramater %s is required", p.Name)
	}
	if !ok && p.DefaultValue != nil {
		p.value = p.DefaultValue
	}

	if p.Unmarshaller != nil {
		// Perform custom unmarshalling
		value, err := p.Unmarshaller(p, tableDriver, raw)
		if err != nil {
			return fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		p.value = value
	} else if p.CastPgType != "" {
		// Perform decoding via pgx driver
		if err := tableDriver.ScanByName(p.CastPgType, raw, p.value); err != nil {
			return fmt.Errorf("unable to scan parameter via Driver")
		}

	} else if p.ColumnParameter != nil {
		if p.Column == nil {
			return fmt.Errorf("parameter is linked but column was not assigned")
		}
		if err := tableDriver.ScanByOid(p.Column.TypeOid, raw, p.value); err != nil {
			return fmt.Errorf("unable to scan parameter via Driver")
		}
	} else {
		// Unmarshal as usual using json Umnarshaler
		if err := json.Unmarshal(raw, p.value); err != nil {
			return fmt.Errorf("unable to unmarshal value: %w", err)
		}
	}

	if p.Validator != nil {
		if err := p.Validator(p.value); err != nil {
			return fmt.Errorf("validation error: %w", err)
		}
	}
	return nil
}

// Scan - scan parsed value into received pointer. Param src must be pointer
func (p *Parameter) Scan(src any) error {
	srcValue := reflect.ValueOf(src)
	destValue := reflect.ValueOf(p.value)
	if srcValue.Kind() == destValue.Kind() {
		srcInd := reflect.Indirect(srcValue)
		destInd := reflect.Indirect(destValue)
		if srcInd.Kind() == destInd.Kind() {
			if srcInd.CanSet() {
				srcInd.Set(destInd)
				return nil
			}
			return errors.New("unable to set the value")
		}
		return errors.New("unexpected src type")
	}
	return errors.New("src must be pointer")
}

func (p *Parameter) SetColumnParameter(cp *Parameter) *Parameter {
	if !p.IsColumn {
		panic("cannot link non column parameter")
	}
	if cp == nil {
		panic("column parameter cannot be nil")
	}
	p.ColumnParameter = p
	return p
}

// Value - returns parsed value that later might be cast via type assertion or so on
func (p *Parameter) Value() any {
	return p.value
}
