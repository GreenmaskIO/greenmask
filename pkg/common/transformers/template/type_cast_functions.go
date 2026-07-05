// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package template

import (
	"slices"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

type dbmsDriver interface {
	GetTypeIDByName(typeName string) (uint32, bool)
	DecodeValueByTypeName(name string, src []byte) (any, error)
	EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error)
}

type columnType struct {
	name string
	oid  uint32
}

type TypeCaster struct {
	cast       TypeCastFunc
	inputType  columnType
	outputType columnType
}

func NewTypeCaster(
	driver dbmsDriver,
	inputType,
	outputType,
	castFunction string,
) (*TypeCaster, core.ValidationWarnings, error) {
	var warnings core.ValidationWarnings
	var castFunc TypeCastFunc

	inputTypeID, ok := driver.GetTypeIDByName(inputType)
	if !ok {
		warnings = append(warnings,
			core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta("RequestedInputType", inputType).
				SetMsg("unsupported input type"),
		)
	}
	outputTypeID, ok := driver.GetTypeIDByName(outputType)
	if !ok {
		warnings = append(warnings,
			core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta("RequestedOutputType", outputType).
				SetMsg("unsupported output type"),
		)
	}

	if inputTypeID == outputTypeID {
		warnings = append(warnings,
			core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta("RequestedOutputType", outputType).
				AddMeta("RequestedInputType", outputType).
				SetMsg("casting is not required for equal type"),
		)
	}

	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	castFuncDef, ok := CastFunctionsMap[castFunction]
	if !ok {
		warnings = append(warnings,
			core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta("CastFuncName", castFunction).
				SetMsg("unable to find cast function"),
		)
	}
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	if !slices.Contains(castFuncDef.InputTypes, inputType) {

		warnings = append(warnings,
			core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta("AllowedInputTypes", castFuncDef.InputTypes).
				AddMeta("RequestedInputType", inputType).
				AddMeta("CastFuncName", castFunction).
				SetMsg("unsupported input type for cast function"),
		)
	}
	if !slices.Contains(castFuncDef.OutputTypes, outputType) {
		warnings = append(warnings,
			core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta("AllowedOutputTypes", castFuncDef.OutputTypes).
				AddMeta("RequestedOutputType", outputType).
				AddMeta("CastFuncName", castFunction).
				SetMsg("unsupported output type for cast function"),
		)
	}
	castFunc = castFuncDef.Cast

	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	return &TypeCaster{
		cast:       castFunc,
		inputType:  columnType{name: inputType, oid: inputTypeID},
		outputType: columnType{name: outputType, oid: outputTypeID},
	}, warnings, nil
}

func (tc *TypeCaster) Cast(driver dbmsDriver, input []byte) (output []byte, err error) {
	return tc.cast(driver, input)
}
