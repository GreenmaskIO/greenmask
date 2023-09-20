package transformers

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var ReplaceTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"Replace",
		"Replace column value to the provided",
		toolkit.TupleTransformation,
	),

	NewReplaceTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"value",
		"value to replace",
		nil,
		nil,
	).SetRequired(true).
		SetLinkParameter("column"),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
		new(bool),
		New(true),
	),
)

type ReplaceTransformer struct {
	columnName string
	keepNull   bool
	value      any
}

func NewReplaceTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {

	var columnName string
	var value any
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	value = parameters["value"].Value()

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &ReplaceTransformer{
		columnName: columnName,
		keepNull:   keepNull,
		value:      value,
	}, nil, nil
}

func (rt *ReplaceTransformer) Init(ctx context.Context) error {
	return nil
}

func (rt *ReplaceTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetAttribute(rt.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rt.keepNull {
		return r, nil
	}

	if err := r.SetAttribute(rt.columnName, rt.value); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(ReplaceTransformerDefinition)
}
