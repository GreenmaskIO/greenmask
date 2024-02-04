package caster_tmp

import (
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type TypeCasterV2 interface {
	Cast(driver *toolkit.Driver, input []byte) (output []byte, err error)
}

type TypeCasterNewFuncV2 func(driver *toolkit.Driver, inputType, outputType string, auto bool, params map[string]any) (TypeCasterV2, error)

type TypeCasterDefinitionV2 struct {
	New             TypeCasterNewFuncV2
	InputTypes      []string
	OutputTypes     []string
	InputTypeClass  *toolkit.TypeClass
	OutputTypeClass *toolkit.TypeClass
}

func (tcd *TypeCasterDefinitionV2) NewTypeCasterV2(driver *toolkit.Driver, inputType, outputType string) (TypeCasterV2, error) {
	if !isTypeCompatible(tcd.InputTypes, tcd.InputTypeClass, inputType) {
		return nil, fmt.Errorf("unsupported input type \"%s\"", inputType)
	}
	if !isTypeCompatible(tcd.OutputTypes, tcd.OutputTypeClass, outputType) {
		return nil, fmt.Errorf("unsupported output type \"%s\"", inputType)
	}
	return tcd.New(driver, inputType, outputType, true, nil)
}

func isTypeCompatible(allowedTypes []string, allowedTypeClass *toolkit.TypeClass, requestedType string) bool {
	return slices.Contains(allowedTypes, requestedType) || slices.Contains(allowedTypeClass.Types, requestedType)
}
