package transformers_new

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/shopspring/decimal"
)

const (
	bigIntTransformerName        = "BigInteger"
	bigIntTransformerDescription = "Generate big integer value in min and max thresholds"
)

const bigIntegerTransformerGenByteLength = 20

func bigIntTypeUnmarshaler(driver *toolkit.Driver, typeName string, v toolkit.ParamsValue) (any, error) {
	res, err := decimal.NewFromString(string(v))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

var bigIntegerTransformerParams = []*toolkit.ParameterDefinition{
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

	toolkit.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),
}

type BigIntegerTransformer struct {
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	t               transformers.Transformer
	dynamicMode     bool
	numericSize     int

	minAllowedValue decimal.Decimal
	maxAllowedValue decimal.Decimal

	columnParam   toolkit.Parameterizer
	maxParam      toolkit.Parameterizer
	minParam      toolkit.Parameterizer
	keepNullParam toolkit.Parameterizer
}

func NewBigIntegerTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer, g generators.Generator) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName string
	var minVal, maxVal decimal.Decimal
	var keepNull, dynamicMode bool

	columnParam := parameters["column"]
	minParam := parameters["min"]
	maxParam := parameters["max"]
	keepNullParam := parameters["keep_null"]

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

	t, err := transformers.NewBigIntTransformer(g, limiter)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing common int transformer: %w", err)
	}

	return &BigIntegerTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
		minAllowedValue: limiter.MinValue,
		maxAllowedValue: limiter.MaxValue,

		columnParam:   columnParam,
		minParam:      minParam,
		maxParam:      maxParam,
		keepNullParam: keepNullParam,
		t:             t,
		numericSize:   c.Length,

		dynamicMode: dynamicMode,
	}, nil, nil
}

func (bit *BigIntegerTransformer) GetAffectedColumns() map[int]string {
	return bit.affectedColumns
}

func (bit *BigIntegerTransformer) Init(ctx context.Context) error {
	return nil
}

func (bit *BigIntegerTransformer) Done(ctx context.Context) error {
	return nil
}

func (bit *BigIntegerTransformer) dynamicTransform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(bit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && bit.keepNull {
		return r, nil
	}

	var minVal, maxVal decimal.Decimal
	err = bit.minParam.Scan(&minVal)
	if err != nil {
		return nil, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = bit.maxParam.Scan(&maxVal)
	if err != nil {
		return nil, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getBigIntLimiterForDynamicParameter(bit.numericSize, minVal, maxVal, bit.minAllowedValue, bit.maxAllowedValue)
	if err != nil {
		return nil, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	ctx = context.WithValue(ctx, "limiter", limiter)
	res, err := bit.t.Transform(ctx, val.Data)
	if err != nil {
		return nil, fmt.Errorf("error generating int value: %w", err)
	}

	if err := r.SetRawColumnValueByIdx(bit.columnIdx, toolkit.NewRawValue(res, false)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func (bit *BigIntegerTransformer) staticTransform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(bit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && bit.keepNull {
		return r, nil
	}
	res, err := bit.t.Transform(ctx, val.Data)
	if err != nil {
		return nil, fmt.Errorf("error generating int value: %w", err)
	}

	if err := r.SetRawColumnValueByIdx(bit.columnIdx, toolkit.NewRawValue(res, false)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func (bit *BigIntegerTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if bit.dynamicMode {
		return bit.dynamicTransform(ctx, r)
	}
	return bit.staticTransform(ctx, r)
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

func init() {

	registerRandomAndDeterministicTransformer(
		utils.DefaultTransformerRegistry,
		bigIntTransformerName,
		bigIntTransformerDescription,
		NewBigIntegerTransformer,
		bigIntegerTransformerParams,
		bigIntegerTransformerGenByteLength,
	)
}
