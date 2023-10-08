package transformers

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

var ReplaceTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"Replace",
		"Replace column value to the provided",
	),

	NewReplaceTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"value",
		"value to replace",
	).SetRequired(true).
		SetLinkParameter("column"),

	toolkit2.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
	).SetDefaultValue(toolkit2.ParamsValue("true")),
)

type ReplaceTransformer struct {
	columnName string
	keepNull   bool
	value      any
}

func NewReplaceTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {

	var columnName string
	var value any
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	value, err := parameters["value"].Value()
	if err != nil {
		return nil, nil, fmt.Errorf(`error getting "value" parameter`)
	}

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

func (rt *ReplaceTransformer) Done(ctx context.Context) error {
	return nil
}

func (rt *ReplaceTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
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
