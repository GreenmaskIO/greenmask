package transformers

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const bigIntegerTransformerGenByteLength = 20

const RandomNumericTransformerName = "RandomNumeric"

var numericTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomNumericTransformerName,
		"Generate numeric value in min and max thresholds",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewRandomNumericTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("numeric", "decimal"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"decimal",
		"the value decimal",
	).SetSupportTemplate(true).
		SetDefaultValue([]byte("0")),

	toolkit.MustNewParameterDefinition(
		"min",
		"min int value threshold",
	).SetLinkParameter("column").
		SetSupportTemplate(true).
		SetRequired(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
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

	toolkit.MustNewParameterDefinition(
		"max",
		"max int value threshold",
	).SetLinkParameter("column").
		SetSupportTemplate(true).
		SetRequired(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
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

	keepNullParameterDefinition,

	engineParameterDefinition,
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

	columnParam    toolkit.Parameterizer
	maxParam       toolkit.Parameterizer
	minParam       toolkit.Parameterizer
	keepNullParam  toolkit.Parameterizer
	engineParam    toolkit.Parameterizer
	precisionParam toolkit.Parameterizer
	transform      func([]byte) (decimal.Decimal, error)
}

func NewRandomNumericTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, engine string
	var minVal, maxVal *decimal.Decimal
	var keepNull, dynamicMode bool
	var precision int32

	columnParam := parameters["column"]
	minParam := parameters["min"]
	maxParam := parameters["max"]
	keepNullParam := parameters["keep_null"]
	engineParam := parameters["engine"]
	precisionParam := parameters["decimal"]

	if err := engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if err := precisionParam.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "decimal" param: %w`, err)
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
		minIsEmpty, err := minParam.IsEmpty()
		if err != nil {
			return nil, nil, fmt.Errorf("error checking \"min\" parameter: %w", err)
		}
		if !minIsEmpty {
			if err = minParam.Scan(&minVal); err != nil {
				return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
			}
		}
		maxIsEmpty, err := maxParam.IsEmpty()
		if err != nil {
			return nil, nil, fmt.Errorf("error checking \"max\" parameter: %w", err)
		}
		if !maxIsEmpty {
			if err = maxParam.Scan(&maxVal); err != nil {
				return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
			}
		}

	}

	limiter, limitsWarnings, err := validateRandomNumericTypeAndSetLimit(bigIntegerTransformerGenByteLength, minVal, maxVal)
	if err != nil {
		return nil, nil, err
	}
	if limitsWarnings.IsFatal() {
		return nil, limitsWarnings, nil
	}
	limiter.SetPrecision(precision)

	t, err := transformers.NewRandomNumericTransformer(limiter, precision)
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

	return &NumericTransformer{
		RandomNumericTransformer: t,
		columnName:               columnName,
		keepNull:                 keepNull,
		affectedColumns:          affectedColumns,
		columnIdx:                idx,
		minAllowedValue:          limiter.MinValue,
		maxAllowedValue:          limiter.MaxValue,

		columnParam:    columnParam,
		minParam:       minParam,
		maxParam:       maxParam,
		keepNullParam:  keepNullParam,
		engineParam:    engineParam,
		precisionParam: precisionParam,
		numericSize:    c.Length,
		transform:      t.Transform,

		dynamicMode: dynamicMode,
	}, nil, nil
}

func (bit *NumericTransformer) GetAffectedColumns() map[int]string {
	return bit.affectedColumns
}

func (bit *NumericTransformer) Init(ctx context.Context) error {
	if bit.dynamicMode {
		bit.transform = bit.dynamicTransform
	}
	return nil
}

func (bit *NumericTransformer) Done(ctx context.Context) error {
	return nil
}

func (bit *NumericTransformer) dynamicTransform(v []byte) (decimal.Decimal, error) {
	var minVal, maxVal decimal.Decimal
	err := bit.minParam.Scan(&minVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = bit.maxParam.Scan(&maxVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getRandomNumericLimiterForDynamicParameter(bit.numericSize, minVal, maxVal, bit.minAllowedValue, bit.maxAllowedValue)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	return bit.RandomNumericTransformer.SetDynamicLimiter(limiter).Transform(v)
}

func (bit *NumericTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(bit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && bit.keepNull {
		return r, nil
	}

	newValue, err := bit.transform(val.Data)
	if err != nil {
		return nil, err
	}

	if err = r.SetColumnValueByIdx(bit.columnIdx, newValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func getNumericThresholds(size int, requestedMinValue, requestedMaxValue *decimal.Decimal,
) (decimal.Decimal, decimal.Decimal, toolkit.ValidationWarnings, error) {
	var warns toolkit.ValidationWarnings
	minVal, maxVal, err := transformers.GetMinAndMaxNumericValueBySetting(size)
	if err != nil {
		return decimal.Decimal{}, decimal.Decimal{}, nil, fmt.Errorf("error creating limiter by size: %w", err)
	}

	if requestedMinValue == nil {
		requestedMinValue = &minVal
	}
	if requestedMaxValue == nil {
		requestedMaxValue = &maxVal
	}

	if !numericLimitIsValid(*requestedMinValue, minVal, maxVal) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested min value is out of numeric(%d) range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", minVal.String()).
			AddMeta("AllowedMaxValue", maxVal.String()).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}

	if !numericLimitIsValid(*requestedMaxValue, minVal, maxVal) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested max value is out of NEMERIC(%d) range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", minVal.String()).
			AddMeta("AllowedMaxValue", maxVal.String()).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}
	if warns.IsFatal() {
		return decimal.Decimal{}, decimal.Decimal{}, warns, nil
	}
	return *requestedMinValue, *requestedMaxValue, nil, nil
}

func validateRandomNumericTypeAndSetLimit(
	size int, requestedMinValue, requestedMaxValue *decimal.Decimal,
) (limiter *transformers.RandomNumericLimiter, warns toolkit.ValidationWarnings, err error) {

	minVal, maxVal, warns, err := getNumericThresholds(size, requestedMinValue, requestedMaxValue)
	if err != nil {
		return nil, nil, err
	}
	if warns.IsFatal() {
		return nil, warns, nil
	}

	limiter, err = transformers.NewRandomNumericLimiter(minVal, maxVal)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating limiter by size: %w", err)
	}

	return limiter, nil, nil
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

func numericTypeUnmarshaler(driver *toolkit.Driver, typeName string, v toolkit.ParamsValue) (any, error) {
	res, err := decimal.NewFromString(string(v))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(numericTransformerDefinition)
}
