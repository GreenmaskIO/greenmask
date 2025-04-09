package parameters

import (
	"fmt"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
)

var (
	errLinkedColumnNameNotSet = fmt.Errorf("linked column name is not set")
)

type StaticParameterContext struct {
	*template.TableDriverContext
	linkedColumnName string
}

func NewStaticParameterContext(td commonininterfaces.TableDriver, linkedColumnName string) *StaticParameterContext {
	return &StaticParameterContext{
		TableDriverContext: template.NewTableDriverContext(td),
		linkedColumnName:   linkedColumnName,
	}
}

func (spc *StaticParameterContext) EncodeValue(v any) (any, error) {
	if spc.linkedColumnName == "" {
		return nil, fmt.Errorf(
			"use .EncodeValueByType or .EncodeValueByColumn instead: %w", errLinkedColumnNameNotSet,
		)
	}
	return spc.EncodeValueByColumn(spc.linkedColumnName, v)
}

func (spc *StaticParameterContext) DecodeValue(v any) (any, error) {
	if spc.linkedColumnName == "" {
		return nil, fmt.Errorf(
			"use .DecodeValueByType or .DecodeValueByColumn instead: %w", errLinkedColumnNameNotSet,
		)
	}
	return spc.DecodeValueByColumn(spc.linkedColumnName, v)
}
