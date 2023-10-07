package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RandomBoolTransformerDefinition = utils.NewDefinition(

	utils.NewTransformerProperties(
		"RandomBool",
		"Generate random bool",
	),

	NewRandomBoolTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("bool"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
		new(bool),
		New(true),
	),
)

type RandomBoolTransformer struct {
	columnName string
	keepNull   bool
	rand       *rand.Rand
}

func NewRandomBoolTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var columnName string
	var keepNull bool
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomBoolTransformer{
		columnName: columnName,
		keepNull:   keepNull,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil
}

func (rbt *RandomBoolTransformer) Init(ctx context.Context) error {
	return nil
}

func (rbt *RandomBoolTransformer) Done(ctx context.Context) error {
	return nil
}

func (rbt *RandomBoolTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	valAny, err := r.GetAttribute(rbt.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rbt.keepNull {
		return r, nil
	}

	if err := r.SetAttribute(rbt.columnName, rbt.rand.Int63n(2) == 1); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomBoolTransformerDefinition)
}
