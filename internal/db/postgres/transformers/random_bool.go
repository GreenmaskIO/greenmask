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
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("bool"),
		).SetRequired(true),
)

type RandomBoolTransformer struct {
	columnName string
	rand       *rand.Rand
}

func NewRandomBoolTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	return &RandomBoolTransformer{
		columnName: columnName,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil
}

func (rbt *RandomBoolTransformer) Init(ctx context.Context) error {
	return nil
}

func (rbt *RandomBoolTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if err := r.SetAttribute(rbt.columnName, rbt.rand.Int63n(2) == 1); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomBoolTransformerDefinition)
}
