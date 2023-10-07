package transformers

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

var SetNullTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"SetNull",
		"Set NULL value",
	),
	NewSetNullTransformer,
	toolkit2.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),
)

type SetNullTransformer struct {
	columnName string
}

func NewSetNullTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
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

func (sut *SetNullTransformer) Done(ctx context.Context) error {
	return nil
}

func (sut *SetNullTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	if err := r.SetAttribute(sut.columnName, toolkit2.NewValue(nil, true)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(SetNullTransformerDefinition)
}
