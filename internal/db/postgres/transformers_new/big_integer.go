package transformers_new

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const bigIntegerTransformerGenByteLength = 20

var bigIntegerTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"BigInteger",
		"Generate big integer value in min and max thresholds",
	),

	NewBigIntegerTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("numeric", "decimal"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min int value threshold",
	).SetLinkParameter("column").
		SetRequired(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("numeric", "decimal", "int2", "int4", "int8").
				SetUnmarshaler(bigIntTypeUnmarshaler),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max int value threshold",
	).SetLinkParameter("column").
		SetRequired(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("numeric", "decimal", "int2", "int4", "int8").
				SetUnmarshaler(bigIntTypeUnmarshaler),
		),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type BigIntegerTransformer struct {
	*transformers.BigIntTransformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	numericSize     int

	minAllowedValue decimal.Decimal
	maxAllowedValue decimal.Decimal

	columnParam   toolkit.Parameterizer
	maxParam      toolkit.Parameterizer
	minParam      toolkit.Parameterizer
	keepNullParam toolkit.Parameterizer
	engineParam   toolkit.Parameterizer
	transform     func(context.Context, []byte) (decimal.Decimal, error)
}

func NewBigIntegerTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, engine string
	var minVal, maxVal decimal.Decimal
	var keepNull, dynamicMode bool

	columnParam := parameters["column"]
	minParam := parameters["min"]
	maxParam := parameters["max"]
	keepNullParam := parameters["keep_null"]
	engineParam := parameters["engine"]

	if err := engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if minParam.IsDynamic() || maxParam.IsDynamic() {
		dynamicMode = true
	}

	if err := columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, c, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	if err := keepNullParam.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	if !dynamicMode {
		if err := minParam.Scan(&minVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
		if err := maxParam.Scan(&maxVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
	}

	limiter, limitsWarnings, err := validateBigIntTypeAndSetLimit(bigIntegerTransformerGenByteLength, minVal, maxVal)
	if err != nil {
		return nil, nil, err
	}
	if limitsWarnings.IsFatal() {
		return nil, limitsWarnings, nil
	}

	t, err := transformers.NewBigIntTransformer(limiter)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing common int transformer: %w", err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &BigIntegerTransformer{
		BigIntTransformer: t,
		columnName:        columnName,
		keepNull:          keepNull,
		affectedColumns:   affectedColumns,
		columnIdx:         idx,
		minAllowedValue:   limiter.MinValue,
		maxAllowedValue:   limiter.MaxValue,

		columnParam:   columnParam,
		minParam:      minParam,
		maxParam:      maxParam,
		keepNullParam: keepNullParam,
		engineParam:   engineParam,
		numericSize:   c.Length,
		transform:     t.Transform,

		dynamicMode: dynamicMode,
	}, nil, nil
}

func (bit *BigIntegerTransformer) GetAffectedColumns() map[int]string {
	return bit.affectedColumns
}

func (bit *BigIntegerTransformer) Init(ctx context.Context) error {
	if bit.dynamicMode {
		bit.transform = bit.dynamicTransform
	}
	return nil
}

func (bit *BigIntegerTransformer) Done(ctx context.Context) error {
	return nil
}

func (bit *BigIntegerTransformer) dynamicTransform(ctx context.Context, v []byte) (decimal.Decimal, error) {
	var minVal, maxVal decimal.Decimal
	err := bit.minParam.Scan(&minVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = bit.maxParam.Scan(&maxVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getBigIntLimiterForDynamicParameter(bit.numericSize, minVal, maxVal, bit.minAllowedValue, bit.maxAllowedValue)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	ctx = context.WithValue(ctx, "limiter", limiter)
	return bit.BigIntTransformer.Transform(ctx, v)
}

func (bit *BigIntegerTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(bit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && bit.keepNull {
		return r, nil
	}

	newValue, err := bit.transform(ctx, val.Data)
	if err != nil {
		return nil, err
	}

	if err = r.SetColumnValueByIdx(bit.columnIdx, newValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func validateBigIntTypeAndSetLimit(
	size int, requestedMinValue, requestedMaxValue decimal.Decimal,
) (limiter *transformers.BigIntLimiter, warns toolkit.ValidationWarnings, err error) {

	limiter, err = transformers.NewBigIntLimiterFromSize(size)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating limiter by size: %w", err)
	}

	if !bigIntLimitIsValid(requestedMinValue, limiter.MinValue, limiter.MaxValue) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested min value is out of numeric(%d) range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", limiter.MinValue.String()).
			AddMeta("AllowedMaxValue", limiter.MaxValue.String()).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}

	if !bigIntLimitIsValid(requestedMaxValue, limiter.MinValue, limiter.MaxValue) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested max value is out of NEMERIC(%d) range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", limiter.MinValue.String()).
			AddMeta("AllowedMaxValue", limiter.MaxValue.String()).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}

	if warns.IsFatal() {
		return nil, warns, nil
	}

	if !requestedMinValue.Equal(decimal.NewFromInt(0)) || !requestedMinValue.Equal(decimal.NewFromInt(0)) {
		limiter, err = transformers.NewBigIntLimiter(requestedMinValue, requestedMaxValue)
		if err != nil {
			return nil, nil, err
		}
	}

	return limiter, nil, nil
}

func bigIntLimitIsValid(requestedThreshold, minValue, maxValue decimal.Decimal) bool {
	return requestedThreshold.GreaterThanOrEqual(minValue) || requestedThreshold.LessThanOrEqual(maxValue)
}

func getBigIntLimiterForDynamicParameter(
	numericSize int, requestedMinValue, requestedMaxValue,
	minAllowedValue, maxAllowedValue decimal.Decimal,
) (*transformers.BigIntLimiter, error) {

	if !bigIntLimitIsValid(requestedMinValue, minAllowedValue, maxAllowedValue) {
		return nil, fmt.Errorf("requested dynamic parameter min value is out of range of NUMERIC(%d) size", numericSize)
	}

	if !bigIntLimitIsValid(requestedMaxValue, minAllowedValue, maxAllowedValue) {
		return nil, fmt.Errorf("requested dynamic parameter max value is out of range of NUMERIC(%d) size", numericSize)
	}

	limiter, err := transformers.NewBigIntLimiter(minAllowedValue, maxAllowedValue)
	if err != nil {
		return nil, err
	}

	if !requestedMinValue.Equal(decimal.NewFromInt(0)) || !requestedMinValue.Equal(decimal.NewFromInt(0)) {
		limiter, err = transformers.NewBigIntLimiter(requestedMinValue, requestedMaxValue)
		if err != nil {
			return nil, err
		}
	}
	return limiter, nil
}

func bigIntTypeUnmarshaler(driver *toolkit.Driver, typeName string, v toolkit.ParamsValue) (any, error) {
	res, err := decimal.NewFromString(string(v))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(bigIntegerTransformerDefinition)
}
