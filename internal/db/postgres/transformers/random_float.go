package transformers

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

const (
	defaultPrecision           int16 = 4
	RandomFloatTransformerName       = "RandomFloat"
)

var RandomFloatTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"RandomFloat",
		"Generate random float",
		toolkit.TupleTransformation,
	),
	NewRandomFloatTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("float4", "float8"),
		).SetRequired(true),
	toolkit.MustNewParameter(
		"min",
		"min int value threshold",
		new(float64),
		nil,
	).SetRequired(true),
	toolkit.MustNewParameter(
		"max",
		"max int value threshold",
		new(float64),
		nil,
	).SetRequired(true),
	toolkit.MustNewParameter(
		"precision",
		"precision of noised value",
		new(int64),
		New[int64](4),
	),
)

type RandomFloatTransformer struct {
	columnName string
	min        float64
	max        float64
	precision  float64
	rand       *rand.Rand
}

func NewRandomFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var minVal, maxVal float64
	var precision int64
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

	return &RandomFloatTransformer{
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

func (rft *RandomFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	resFloat := rft.min + rft.rand.Float64()*(rft.max-rft.min)
	resFloat = round(resFloat, rft.precision)

	if err := r.SetAttribute(rft.columnName, &resFloat); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(RandomFloatTransformerDefinition)
}
