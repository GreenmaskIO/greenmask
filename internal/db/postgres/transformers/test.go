package transformers

import (
	"context"
	"time"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

var TestTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties("Test", "simple description", toolkit.TupleTransformation),
	NewTestTransformerPlaceholder,
	toolkit.MustNewParameter("column", "a column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("timestamp", "timestamptz"),
		),
	toolkit.MustNewParameter("replace", "replacement value", &time.Time{}, nil).
		SetLinkParameter("column"),
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

func init() {
	DefaultTransformerRegistry.MustRegister(TestTransformerDefinition)
}
