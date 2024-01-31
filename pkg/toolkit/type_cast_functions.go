package toolkit

import (
	"fmt"
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
	var autoCast bool
	var castFunc TypeCastFunc
	var err error

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

	if castFunction != "" {
		autoCast = false
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
	}

	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	var autoCastWarns ValidationWarnings
	if autoCast {
		castFunc, autoCastWarns, err = makeCastDecision(driver, inputPgType, outputPgType)
		warnings = append(warnings, autoCastWarns...)
		if err != nil {
			return nil, warnings, fmt.Errorf("unable to make a auto cast decision: %w", err)
		}
	}

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

func makeCastDecision(driver *Driver, inputPgType, outputPgType *pgtype.Type) (
	castFunc TypeCastFunc, warns ValidationWarnings, err error,
) {
	// 1. Determine inputType and outputClasses Class
	// 2. Check that type cast can be determined accurately via dynamic casting map (ints -> dates | dates -> ints, etc.)
	//	2.1 Determine via Class first if exists
	//	2.2 Use common type if type does not have a Class
	// 3. If type can be determined via dynamic casting map, then return dynamic functions. Dynamic function must be
	//		able to validate the value and make a strong decision for which domain it belongs. For instance cast
	//	    int Unix (sec, ms, ml, ns) to Date
	// 4. If type does not have a dynamic function then try to find	any function that has intersection with inp and out
	//    types
	// 5. Return the found cast function

}
