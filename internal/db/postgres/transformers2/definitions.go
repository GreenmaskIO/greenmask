package transformers2

import (
	"context"
	"time"

	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

var TestTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewProperties("test", "simple description", toolkit.TupleTransformation),
	NewTestTransformerPlaceholder,
	[]*toolkit.Parameter{
		toolkit.MustNewParameter("column", "a column name", new(string), nil).
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetAllowedColumnTypes("timestamp"),
			),
		toolkit.MustNewParameter("replace", "replacement value", &time.Time{}, nil).
			SetLinkParameter("column"),
	},
)

type TestTransformerPlaceholder struct {
	p map[string]*toolkit.Parameter
}

func NewTestTransformerPlaceholder(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, error) {
	return &TestTransformerPlaceholder{
		p: parameters,
	}, nil
}

func (tt *TestTransformerPlaceholder) Init(ctx context.Context) error {
	return nil
}

func (tt *TestTransformerPlaceholder) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}

func (tt *TestTransformerPlaceholder) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	return r, nil
}
