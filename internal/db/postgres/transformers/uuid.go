package transformers

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var RandomUuidTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"RandomUuid",
		"Generate random uuid",
		toolkit.TupleTransformation,
	),

	NewRandomUuidTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar", "uuid"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
		new(bool),
		New(false),
	),
)

type RandomUuidTransformer struct {
	columnName string
	keepNull   bool
}

func NewRandomUuidTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomUuidTransformer{
		columnName: columnName,
		keepNull:   keepNull,
	}, nil, nil
}

func (rut *RandomUuidTransformer) Init(ctx context.Context) error {
	return nil
}

func (rut *RandomUuidTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetAttribute(rut.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rut.keepNull {
		return r, nil
	}

	if err = r.SetAttribute(rut.columnName, uuid.New()); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomUuidTransformerDefinition)
}
