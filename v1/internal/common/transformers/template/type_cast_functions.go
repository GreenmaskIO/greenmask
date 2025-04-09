package template

import (
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type dbmsDriver interface {
	GetTypeOIDByName(typeName string) (uint32, bool)
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
) (*TypeCaster, models.ValidationWarnings, error) {
	var warnings models.ValidationWarnings
	var castFunc TypeCastFunc

	inputTypeOID, ok := driver.GetTypeOIDByName(inputType)
	if !ok {
		warnings = append(warnings,
			models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta("RequestedInputType", inputType).
				SetMsg("unsupported input type"),
		)
	}
	outputTypeOID, ok := driver.GetTypeOIDByName(outputType)
	if !ok {
		warnings = append(warnings,
			models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta("RequestedOutputType", outputType).
				SetMsg("unsupported output type"),
		)
	}

	if inputTypeOID == outputTypeOID {
		warnings = append(warnings,
			models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
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
			models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta("CastFuncName", castFunction).
				SetMsg("unable to find cast function"),
		)
	}
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	if !slices.Contains(castFuncDef.InputTypes, inputType) {

		warnings = append(warnings,
			models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta("AllowedInputTypes", castFuncDef.InputTypes).
				AddMeta("RequestedInputType", inputType).
				AddMeta("CastFuncName", castFunction).
				SetMsg("unsupported input type for cast function"),
		)
	}
	if !slices.Contains(castFuncDef.OutputTypes, outputType) {
		warnings = append(warnings,
			models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
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
		inputType:  columnType{name: inputType, oid: inputTypeOID},
		outputType: columnType{name: outputType, oid: outputTypeOID},
	}, warnings, nil
}

func (tc *TypeCaster) Cast(driver dbmsDriver, input []byte) (output []byte, err error) {
	return tc.cast(driver, input)
}
