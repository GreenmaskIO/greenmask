package transformers

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var RandomUuidTransformerDefinition = transformers.NewDefinition(
	transformers.MustNewTransformerProperties(
		"RandomUuid",
		"Generate random uuid",
		transformers.TupleTransformation,
	),
	NewRandomUuidTransformer,
	transformers.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(transformers.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("text", "varchar", "uuid"),
		).SetRequired(true),
)

type RandomUuidTransformer struct {
	columnName string
}

func NewRandomUuidTransformer(ctx context.Context, driver *transformers.Driver, parameters map[string]*transformers.Parameter) (transformers.Transformer, transformers.ValidationWarnings, error) {
	var columnName string

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	return &RandomUuidTransformer{
		columnName: columnName,
	}, nil, nil
}

func (rut *RandomUuidTransformer) Init(ctx context.Context) error {
	return nil
}

func (rut *RandomUuidTransformer) Validate(ctx context.Context) (transformers.ValidationWarnings, error) {
	return nil, nil
}

func (rut *RandomUuidTransformer) Transform(ctx context.Context, r *transformers.Record) (*transformers.Record, error) {
	if err := r.SetAttribute(rut.columnName, uuid.New()); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(RandomUuidTransformerDefinition)
}
