package transformers

import (
	"context"
	"fmt"

	"github.com/GreenmaskIO/greenmask/pkg/toolkit/transformers"
)

var SetNullTransformerDefinition = transformers.NewDefinition(
	transformers.MustNewTransformerProperties(
		"SetNull",
		"Set NULL value",
		transformers.TupleTransformation,
	),
	NewSetNullTransformer,
	transformers.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(transformers.NewColumnProperties().
			SetAffected(true),
		).SetRequired(true),
)

type SetNullTransformer struct {
	columnName string
}

func NewSetNullTransformer(ctx context.Context, driver *transformers.Driver, parameters map[string]*transformers.Parameter) (transformers.Transformer, transformers.ValidationWarnings, error) {
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

func (sut *SetNullTransformer) Transform(ctx context.Context, r *transformers.Record) (*transformers.Record, error) {
	if err := r.SetAttribute(sut.columnName, transformers.DefaultNullSeq); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(SetNullTransformerDefinition)
}
