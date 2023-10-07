package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RandomIntTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomInt",
		"Generate random int value from min to max",
	),

	NewRandomIntTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("int2", "int4", "int8"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"min",
		"min int value threshold",
		new(int64),
		nil,
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"max",
		"max int value threshold",
		new(int64),
		nil,
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
		new(bool),
		New(true),
	),
)

type RandomIntTransformer struct {
	columnName string
	keepNull   bool
	min        int64
	max        int64
	rand       *rand.Rand
}

func NewRandomIntTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var columnName string
	var minVal, maxVal int64
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

	if minVal >= maxVal {
		return nil, toolkit2.ValidationWarnings{
			toolkit2.NewValidationWarning().
				AddMeta("min", minVal).
				AddMeta("max", maxVal).
				SetMsg("max value must be greater that min value"),
		}, nil
	}

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomIntTransformer{
		columnName: columnName,
		keepNull:   keepNull,
		min:        minVal,
		max:        maxVal,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil
}

func (rit *RandomIntTransformer) Init(ctx context.Context) error {
	return nil
}

func (rit *RandomIntTransformer) Done(ctx context.Context) error {
	return nil
}

func (rit *RandomIntTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	valAny, err := r.GetAttribute(rit.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rit.keepNull {
		return r, nil
	}

	res := rit.rand.Int63n(rit.max-rit.min) + rit.min

	if err := r.SetAttribute(rit.columnName, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomIntTransformerDefinition)
}
