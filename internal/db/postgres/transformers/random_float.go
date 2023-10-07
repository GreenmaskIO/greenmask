package transformers

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
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

	toolkit2.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"min",
		"min int value threshold",
		new(float64),
		nil,
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"max",
		"max int value threshold",
		new(float64),
		nil,
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"precision",
		"precision of noised value",
		new(int64),
		New[int64](4),
	),

	toolkit2.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
		new(bool),
		New(true),
	),
)

type RandomFloatTransformer struct {
	columnName string
	keepNull   bool
	min        float64
	max        float64
	precision  float64
	rand       *rand.Rand
}

func NewRandomFloatTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var columnName string
	var minVal, maxVal float64
	var precision int64
	var keepNull bool
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

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
		keepNull:   keepNull,
		precision:  math.Pow(10, float64(precision)),
		min:        minVal,
		max:        maxVal,
		columnName: columnName,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil

}

func (rft *RandomFloatTransformer) Init(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	valAny, err := r.GetAttribute(rft.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rft.keepNull {
		return r, nil
	}

	resFloat := rft.min + rft.rand.Float64()*(rft.max-rft.min)
	resFloat = round(resFloat, rft.precision)

	if err := r.SetAttribute(rft.columnName, &resFloat); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomFloatTransformerDefinition)
}
