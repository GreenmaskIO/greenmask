package transformers

import (
	"context"
	"fmt"

	"github.com/GreenmaskIO/greenmask/pkg/toolkit/transformers"
)

var ReplaceTransformerDefinition = transformers.NewDefinition(
	transformers.MustNewTransformerProperties(
		"Replace",
		"Replace column value to the provided",
		transformers.TupleTransformation,
	),
	NewReplaceTransformer,
	transformers.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(transformers.NewColumnProperties().
			SetAffected(true),
		).SetRequired(true),
	transformers.MustNewParameter(
		"value",
		"value to replace",
		nil,
		nil,
	).SetRequired(true).
		SetLinkParameter("column"),
)

type ReplaceTransformer struct {
	columnName string
	value      any
}

func NewReplaceTransformer(ctx context.Context, driver *transformers.Driver, parameters map[string]*transformers.Parameter) (transformers.Transformer, transformers.ValidationWarnings, error) {

	var columnName string
	var value any

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	value = parameters["value"].Value()

	return &ReplaceTransformer{
		columnName: columnName,
		value:      value,
	}, nil, nil
}

func (rt *ReplaceTransformer) Init(ctx context.Context) error {
	return nil
}

func (rt *ReplaceTransformer) Validate(ctx context.Context) (transformers.ValidationWarnings, error) {
	return nil, nil
}

func (rt *ReplaceTransformer) Transform(ctx context.Context, r *transformers.Record) (*transformers.Record, error) {
	if err := r.SetAttribute(rt.columnName, rt.value); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(ReplaceTransformerDefinition)
}
