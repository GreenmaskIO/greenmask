package transformers

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators/transformers"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const bigIntegerTransformerGenByteLength = 20

const RandomNumericTransformerName = "RandomNumeric"

var RandomNumericTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RandomNumericTransformerName,
		"Generate numeric value in min and max thresholds",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewRandomNumericTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		commonparameters.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("numeric", "decimal"),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"decimal",
		"the value decimal",
	).SetSupportTemplate(true).
		SetDefaultValue([]byte("0")),

	commonparameters.MustNewParameterDefinition(
		"min",
		"min int value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetRequired(true).
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes(
					"numeric",
					"decimal",
					"int2",
					"int4",
					"int8",
					"float4",
					"float8",
				).
				SetUnmarshaler(numericTypeUnmarshaler),
		),

	commonparameters.MustNewParameterDefinition(
		"max",
		"max int value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetRequired(true).
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes(
					"numeric",
					"decimal",
					"int2",
					"int4",
					"int8",
					"float4",
					"float8",
				).
				SetUnmarshaler(numericTypeUnmarshaler),
		),

	commonparameters.MustNewParameterDefinition(
		"type_size",
		"size of the numeric type (total number of digits)",
	).SetDefaultValue(commonmodels.ParamsValue("4")),

	commonparameters.MustNewParameterDefinition(
		"decimal",
		"Number of decimal places to use",
	).SetSupportTemplate(true).
		SetDefaultValue(commonmodels.ParamsValue("4")),

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

// TODO: Add numeric introspection (getting the Numering settings)
type NumericTransformer struct {
	*transformers.RandomNumericTransformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	numericSize     int

	minAllowedValue decimal.Decimal
	maxAllowedValue decimal.Decimal

	maxParam  commonparameters.Parameterizer
	minParam  commonparameters.Parameterizer
	transform func([]byte) (decimal.Decimal, error)
}

func NewRandomNumericTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	var minVal, maxVal *decimal.Decimal

	minParam := parameters["min"]
	maxParam := parameters["max"]

	dynamicMode := isInDynamicMode(parameters)

	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	decimalVal, err := getParameterValueWithName[int32](ctx, parameters, "decimal")
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	typeSize := column.Length
	if typeSize == 0 {
		log.Ctx(ctx).
			Info().
			Msg("unable to detect float size from column length, trying to get it from \"type_size\" parameter")
		typeSize, err = getParameterValueWithName[int](
			ctx,
			parameters,
			"type_size",
		)
		if err != nil {
			return nil, fmt.Errorf("scan \"type_size\" param: %w", err)
		}
		log.Ctx(ctx).
			Info().
			Msgf("using float size %d from \"type_size\" parameter", typeSize)
	}

	if !dynamicMode {
		minVal, maxVal, err = getOptionalMinAndMaxThresholds[decimal.Decimal](minParam, maxParam)
		if err != nil {
			return nil, fmt.Errorf("get min and max thresholds: %w", err)
		}
	}

	limiter, err := validateRandomNumericTypeAndSetLimit(ctx, bigIntegerTransformerGenByteLength, minVal, maxVal)
	if err != nil {
		return nil, fmt.Errorf("validate numeric type and set limit: %w", err)
	}
	limiter.SetPrecision(decimalVal)

	t, err := transformers.NewRandomNumericTransformer(limiter, decimalVal)
	if err != nil {
		return nil, fmt.Errorf("new random numeric transformer: %w", err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	return &NumericTransformer{
		RandomNumericTransformer: t,
		columnName:               columnName,
		keepNull:                 keepNull,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx:       column.Idx,
		minAllowedValue: limiter.MinValue,
		maxAllowedValue: limiter.MaxValue,

		minParam: minParam,
		maxParam: maxParam,
		// TODO: The driver that is implemented must support getting the numeric size.
		numericSize: typeSize,
		transform:   t.Transform,

		dynamicMode: dynamicMode,
	}, nil
}

func (t *NumericTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *NumericTransformer) Init(context.Context) error {
	if t.dynamicMode {
		t.transform = t.dynamicTransform
	}
	return nil
}

func (t *NumericTransformer) Done(context.Context) error {
	return nil
}

func (t *NumericTransformer) dynamicTransform(v []byte) (decimal.Decimal, error) {
	var minVal, maxVal decimal.Decimal
	err := t.minParam.Scan(&minVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = t.maxParam.Scan(&maxVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getRandomNumericLimiterForDynamicParameter(t.numericSize, minVal, maxVal, t.minAllowedValue, t.maxAllowedValue)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	return t.RandomNumericTransformer.SetDynamicLimiter(limiter).Transform(v)
}

func (t *NumericTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return nil
	}

	newValue, err := t.transform(val.Data)
	if err != nil {
		return err
	}

	if err = r.SetColumnValueByIdx(t.columnIdx, newValue); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func getNumericThresholds(ctx context.Context, size int, requestedMinValue, requestedMaxValue *decimal.Decimal,
) (decimal.Decimal, decimal.Decimal, error) {
	minVal, maxVal, err := transformers.GetMinAndMaxNumericValueBySetting(size)
	if err != nil {
		return decimal.Decimal{}, decimal.Decimal{}, fmt.Errorf("get limiter by size: %w", err)
	}

	if requestedMinValue == nil {
		requestedMinValue = &minVal
	}
	if requestedMaxValue == nil {
		requestedMaxValue = &maxVal
	}

	if !numericLimitIsValid(*requestedMinValue, minVal, maxVal) {
		validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
			SetMsgf("requested min value is out of NUMERIC(%d) range", size).
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta("AllowedMinValue", minVal.String()).
			AddMeta("AllowedMaxValue", maxVal.String()).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue))
		return decimal.Decimal{}, decimal.Decimal{}, commonmodels.ErrFatalValidationError
	}

	if !numericLimitIsValid(*requestedMaxValue, minVal, maxVal) {
		validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
			SetMsgf("requested max value is out of NUMERIC(%d) range", size).
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta("AllowedMinValue", minVal.String()).
			AddMeta("AllowedMaxValue", maxVal.String()).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue))
		return decimal.Decimal{}, decimal.Decimal{}, commonmodels.ErrFatalValidationError
	}
	return *requestedMinValue, *requestedMaxValue, nil
}

func validateRandomNumericTypeAndSetLimit(
	ctx context.Context, size int, requestedMinValue, requestedMaxValue *decimal.Decimal,
) (limiter *transformers.RandomNumericLimiter, err error) {
	minVal, maxVal, err := getNumericThresholds(ctx, size, requestedMinValue, requestedMaxValue)
	if err != nil {
		return nil, err
	}

	limiter, err = transformers.NewRandomNumericLimiter(minVal, maxVal)
	if err != nil {
		return nil, fmt.Errorf("error creating limiter by size: %w", err)
	}

	return limiter, nil
}

func numericLimitIsValid(requestedThreshold, minValue, maxValue decimal.Decimal) bool {
	return requestedThreshold.GreaterThanOrEqual(minValue) || requestedThreshold.LessThanOrEqual(maxValue)
}

func getRandomNumericLimiterForDynamicParameter(
	numericSize int, requestedMinValue, requestedMaxValue,
	minAllowedValue, maxAllowedValue decimal.Decimal,
) (*transformers.RandomNumericLimiter, error) {

	if !numericLimitIsValid(requestedMinValue, minAllowedValue, maxAllowedValue) {
		return nil, fmt.Errorf("requested dynamic parameter min value is out of range of NUMERIC(%d) size", numericSize)
	}

	if !numericLimitIsValid(requestedMaxValue, minAllowedValue, maxAllowedValue) {
		return nil, fmt.Errorf("requested dynamic parameter max value is out of range of NUMERIC(%d) size", numericSize)
	}

	limiter, err := transformers.NewRandomNumericLimiter(minAllowedValue, maxAllowedValue)
	if err != nil {
		return nil, err
	}

	if !requestedMinValue.Equal(decimal.NewFromInt(0)) || !requestedMaxValue.Equal(decimal.NewFromInt(0)) {
		limiter, err = transformers.NewRandomNumericLimiter(requestedMinValue, requestedMaxValue)
		if err != nil {
			return nil, err
		}
	}
	return limiter, nil
}

func numericTypeUnmarshaler(_ commonininterfaces.DBMSDriver, _ string, v commonmodels.ParamsValue) (any, error) {
	res, err := decimal.NewFromString(string(v))
	if err != nil {
		return nil, err
	}
	return &res, nil
}
