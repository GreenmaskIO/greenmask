package transformers2

import (
	"context"
	"fmt"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

var (
	ParameterNameKeepNull  = "keep_null"
	ParameterNameColumn    = "column"
	ParameterValueValidate = "validate"
)

// TransformationFunc - a transformation function. It has the same signature as
// commonininterfaces.Transformer.Transform method.
type TransformationFunc func(_ context.Context, r commonininterfaces.Recorder) error

// TransformWithKeepNull - wrapper that simplifies the logic of keep null parameter. You can set
// the keep_null logic on transformer initialization. Just provide the main transformation function
// and the columnIdx (the index of the column to be transformed).
func TransformWithKeepNull(tf TransformationFunc, columnIdx int) TransformationFunc {
	return func(ctx context.Context, r commonininterfaces.Recorder) error {
		isNull, err := r.IsNullByColumnIdx(columnIdx)
		if err != nil {
			return fmt.Errorf("unable to scan column value: %w", err)
		}
		if isNull {
			// If is null and need to keep null - do not change a record.
			return nil
		}
		return tf(ctx, r)
	}
}

// panicParameterDoesNotExists - panic helper for case when parameter is not found in the map.
// It is used everywhere in get helpers below.
func panicParameterDoesNotExists(parameterName string) {
	panic(
		fmt.Errorf(`parameter "%s" is not found: %w`,
			parameterName,
			commonmodels.ErrCheckTransformerImplementation),
	)
}

// panicParameterDoesNotExists - returns the parameter value by scanning a value into variable.
// The type is provided via generic parameter.
func getParameterValueWithName[T any](
	vc *validationcollector.Collector,
	parameters map[string]commonparameters.Parameterizer,
	parameterName string,
) (T, error) {
	parameter, ok := parameters[parameterName]
	if !ok {
		panicParameterDoesNotExists(parameterName)
	}
	var res T
	if err := parameter.Scan(&res); err != nil {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta(commonmodels.MetaKeyParameterName, parameterName).
			SetError(err).
			SetMsg("error scanning parameter"))
		return res, commonmodels.ErrFatalValidationError
	}
	return res, nil
}

// getColumnParameterValueWithName - simplifies the logic of common column parameter.
// It gets the column name, get column definition.
func getColumnParameterValueWithName(
	vc *validationcollector.Collector,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
	parameterName string,
) (string, *commonmodels.Column, error) {
	columnName, err := getParameterValueWithName[string](vc, parameters, parameterName)
	if err != nil {
		return "", nil, err
	}
	c, err := tableDriver.GetColumnByName(columnName)
	if err != nil {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta(commonmodels.MetaKeyParameterName, parameterName).
			AddMeta(commonmodels.MetaKeyParameterValue, columnName).
			SetError(err).
			SetMsg("error getting column value"))
		return "", nil, commonmodels.ErrFatalValidationError
	}
	return columnName, c, nil
}

// getColumnParameterValue - get a column parameter value with name "column". It does the same
// as getColumnParameterValueWithName helper.
func getColumnParameterValue(
	vc *validationcollector.Collector,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (string, *commonmodels.Column, error) {
	return getColumnParameterValueWithName(vc, tableDriver, parameters, ParameterNameColumn)
}
