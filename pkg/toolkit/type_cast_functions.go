package toolkit

import (
	"slices"

	"github.com/jackc/pgx/v5/pgtype"
)

type TypeCaster struct {
	cast        TypeCastFunc
	inputPgType *pgtype.Type
	outputPgTyp *pgtype.Type
}

func NewTypeCaster(driver *Driver, inputType, outputType, castFunction string) (*TypeCaster, ValidationWarnings, error) {
	var warnings ValidationWarnings
	var castFunc TypeCastFunc

	inputPgType, ok := driver.GetTypeMap().TypeForName(inputType)
	if !ok {
		warnings = append(warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				AddMeta("RequestedInputType", inputType).
				SetMsg("unsupported input type"),
		)
	}
	outputPgType, ok := driver.GetTypeMap().TypeForName(outputType)
	if !ok {
		warnings = append(warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				AddMeta("RequestedOutputType", outputType).
				SetMsg("unsupported output type"),
		)
	}

	if inputPgType.OID == outputPgType.OID {
		warnings = append(warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
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
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				AddMeta("CastFuncName", castFunction).
				SetMsg("unable to find cast function"),
		)
	}
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	if !slices.Contains(castFuncDef.InputTypes, inputType) {

		warnings = append(warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				AddMeta("AllowedInputTypes", castFuncDef.InputTypes).
				AddMeta("RequestedInputType", inputType).
				AddMeta("CastFuncName", castFunction).
				SetMsg("unsupported input type for cast function"),
		)
	}
	if !slices.Contains(castFuncDef.OutputTypes, outputType) {
		warnings = append(warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
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
		cast:        castFunc,
		inputPgType: inputPgType,
		outputPgTyp: outputPgType,
	}, warnings, nil
}

func (tc *TypeCaster) Cast(driver *Driver, input []byte) (output []byte, err error) {
	return tc.cast(driver, input)
}
