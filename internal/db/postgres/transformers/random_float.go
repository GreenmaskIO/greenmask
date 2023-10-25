package transformers

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	defaultPrecision           int16 = 4
	RandomFloatTransformerName       = "RandomFloat"
)

var RandomFloatTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomFloat",
		"Generate random float",
	),

	NewRandomFloatTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"min",
		"min int value threshold",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"max",
		"max int value threshold",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"precision",
		"precision of noised value",
	).SetDefaultValue(toolkit.ParamsValue("4")),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type RandomFloatTransformer struct {
	columnName      string
	keepNull        bool
	min             float64
	max             float64
	precision       float64
	rand            *rand.Rand
	affectedColumns map[int]string
}

func NewRandomFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var minVal, maxVal float64
	var precision int64
	var keepNull bool
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["min"]
	if err := p.Scan(&minVal); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	p = parameters["max"]
	if err := p.Scan(&maxVal); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	p = parameters["precision"]
	if err := p.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomFloatTransformer{
		keepNull:        keepNull,
		precision:       math.Pow(10, float64(precision)),
		min:             minVal,
		max:             maxVal,
		columnName:      columnName,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
	}, nil, nil

}

func (rft *RandomFloatTransformer) GetAffectedColumns() map[int]string {
	return rft.affectedColumns
}

func (rft *RandomFloatTransformer) Init(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetRawAttributeValueByName(rft.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rft.keepNull {
		return r, nil
	}

	resFloat := rft.min + rft.rand.Float64()*(rft.max-rft.min)
	resFloat = round(resFloat, rft.precision)

	if err := r.SetAttributeByName(rft.columnName, &resFloat); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomFloatTransformerDefinition)
}
