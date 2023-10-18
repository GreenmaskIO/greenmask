package transformers

import (
	"context"
	"fmt"

	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/tidwall/sjson"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
)

var JsonTransformerDefinition = utils.NewDefinition(

	utils.NewTransformerProperties(
		"Json",
		"Update json document",
	),

	NewJsonTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("json", "jsonb"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"operations",
		"list of the operations",
	).SetRequired(true),
)

type Operation struct {
	Operation string `mapstructure:"operation" validate:"required, oneof=delete set"`
	//TypeName      string      `mapstructure:"type,omitempty" validate:"required, oneof=nil bool string int float "`
	Value interface{} `mapstructure:"value,omitempty"`
	Path  string      `mapstructure:"path" validate:"required"`
}

func (o *Operation) Apply(inp string) (string, error) {
	var val string
	var err error
	if o.Operation == "set" {
		val, err = sjson.Set(inp, o.Path, o.Value)
	} else if o.Operation == "delete" {
		val, err = sjson.Delete(inp, o.Path)
	} else {
		return "", fmt.Errorf("unknown operation %s", o.Operation)
	}
	if err != nil {
		return "", fmt.Errorf("cannot %s value: %w", o.Operation, err)
	}
	return val, nil
}

type JsonTransformer struct {
	columnName      string
	operations      []Operation
	affectedColumns map[int]string
}

func NewJsonTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var ops []Operation
	var columnName string

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["operations"]
	if err := p.Scan(&ops); err != nil {
		return nil, nil, fmt.Errorf("unable to parse operations param: %w", err)
	}

	return &JsonTransformer{
		columnName:      columnName,
		operations:      ops,
		affectedColumns: affectedColumns,
	}, nil, nil
}

func (jt *JsonTransformer) GetAffectedColumns() map[int]string {
	return jt.affectedColumns
}

func (jt *JsonTransformer) Init(ctx context.Context) error {
	return nil
}

func (jt *JsonTransformer) Done(ctx context.Context) error {
	return nil
}

func (jt *JsonTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	// TODO: Test whats happen if performed operation is not applied due to unknown path
	var jsonRawValue string
	isNull, err := r.ScanAttributeByName(jt.columnName, &jsonRawValue)
	if err != nil {
		return nil, fmt.Errorf("cannot scan column value: %w", err)
	}
	if isNull {
		return r, nil
	}

	for _, op := range jt.operations {
		jsonRawValue, err = op.Apply(jsonRawValue)
		if err != nil {
			return nil, fmt.Errorf("cannot apply operation to the json value: %s: %s: %s", op.Operation, op.Path, op.Value)
		}
	}

	if err = r.SetAttributeByName(jt.columnName, jsonRawValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(JsonTransformerDefinition)
}
