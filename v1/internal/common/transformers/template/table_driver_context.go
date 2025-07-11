package template

import (
	"fmt"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
)

var (
	errColumnNotFound = fmt.Errorf("column not found")
)

type TableDriverContext struct {
	td commonininterfaces.TableDriver
}

func NewTableDriverContext(td commonininterfaces.TableDriver) *TableDriverContext {
	return &TableDriverContext{
		td: td,
	}
}

// EncodeValueByColumn - encode value from real type to the string or NullValue using column type
func (tdc *TableDriverContext) EncodeValueByColumn(name string, v any) (any, error) {
	if _, ok := v.(NullType); ok {
		return NullValue, nil
	}

	res, err := tdc.td.EncodeValueByColumnName(name, v, nil)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// DecodeValueByColumn - decode value from string or NullValue to the real type using column type
func (tdc *TableDriverContext) DecodeValueByColumn(name string, v any) (any, error) {
	switch vv := v.(type) {
	case NullType:
		return NullValue, nil
	case string:
		res, err := tdc.td.DecodeValueByColumnName(name, []byte(vv))
		if err != nil {
			return nil, err
		}
		return castToDefault(res), nil
	default:
		return "", fmt.Errorf("unable to decode value %+v by column  \"%s\"", vv, name)
	}
}

// EncodeValueByType - encode value from real type to the string or NullValue using type
func (tdc *TableDriverContext) EncodeValueByType(name string, v any) (any, error) {
	realName, ok := typeAliases[name]
	if ok {
		name = realName
	}

	if _, ok := v.(NullType); ok {
		return NullValue, nil
	}

	res, err := tdc.td.EncodeValueByTypeName(name, v, nil)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// DecodeValueByType - decode value from string or NullValue to the real type using type
func (tdc *TableDriverContext) DecodeValueByType(name string, v any) (any, error) {
	realName, ok := typeAliases[name]
	if ok {
		name = realName
	}

	switch vv := v.(type) {
	case NullType:
		return NullValue, nil
	case string:
		res, err := tdc.td.DecodeValueByTypeName(name, []byte(vv))
		if err != nil {
			return nil, err
		}
		return castToDefault(res), nil
	default:
		return "", fmt.Errorf("unable to decode value %+v by type \"%s\"", vv, name)
	}
}

func (tdc *TableDriverContext) GetColumnType(name string) (string, error) {
	c, err := tdc.td.GetColumnByName(name)
	if err != nil {
		return "", err
	}
	return c.TypeName, nil
}
