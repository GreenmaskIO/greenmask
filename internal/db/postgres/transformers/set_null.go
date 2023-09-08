package transformers

import (
	"context"
	"fmt"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

var SetNullTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"SetNull",
		"Set NULL value",
		toolkit.TupleTransformation,
	),
	NewSetNullTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true),
		).SetRequired(true),
)

type SetNullTransformer struct {
	columnName string
}

func NewSetNullTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	return &SetNullTransformer{
		columnName: columnName,
	}, nil, nil
}

func (sut *SetNullTransformer) Init(ctx context.Context) error {
	return nil
}

func (sut *SetNullTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if err := r.SetAttribute(sut.columnName, toolkit.DefaultNullSeq); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(SetNullTransformerDefinition)
}
