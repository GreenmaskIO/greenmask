package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var RandomBoolTransformerDefinition = toolkit.NewDefinition(

	toolkit.MustNewTransformerProperties(
		"RandomBool",
		"Generate random bool",
		toolkit.TupleTransformation,
	),

	NewRandomBoolTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("bool"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"keepNull",
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

func NewRandomBoolTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var keepNull bool
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	p = parameters["keepNull"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keepNull" param: %w`, err)
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

func (rbt *RandomBoolTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if r.IsNull(rbt.columnName) && rbt.keepNull {
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
