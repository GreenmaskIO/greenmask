package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var RandomIntTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"RandomInt",
		"Generate random int value from min to max",
		toolkit.TupleTransformation,
	),
	NewRandomIntTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("int2", "int4", "int8"),
		).SetRequired(true),
	toolkit.MustNewParameter(
		"min",
		"min int value threshold",
		new(int64),
		nil,
	).SetRequired(true),
	toolkit.MustNewParameter(
		"max",
		"max int value threshold",
		new(int64),
		nil,
	).SetRequired(true),
)

type RandomIntTransformer struct {
	columnName string
	min        int64
	max        int64
	rand       *rand.Rand
}

func NewRandomIntTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var minVal, maxVal int64
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

	if minVal >= maxVal {
		return nil, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				AddMeta("min", minVal).
				AddMeta("max", maxVal).
				SetMsg("max value must be greater that min value"),
		}, nil
	}

	return &RandomIntTransformer{
		columnName: columnName,
		min:        minVal,
		max:        maxVal,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil
}

func (rit *RandomIntTransformer) Init(ctx context.Context) error {
	return nil
}

func (rit *RandomIntTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}

func (rit *RandomIntTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	res := rit.rand.Int63n(rit.max-rit.min) + rit.min

	if err := r.SetAttribute(rit.columnName, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(RandomIntTransformerDefinition)
}
