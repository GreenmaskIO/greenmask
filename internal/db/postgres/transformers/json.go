package transformers

import (
	"context"
	"fmt"

	"github.com/tidwall/sjson"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

const (
	JsonTransformerName = "Json"
)

var JsonTransformerDefinition = toolkit.NewDefinition(

	toolkit.MustNewTransformerProperties(
		"Json",
		"Update json document",
		toolkit.TupleTransformation,
	),
	NewJsonTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("json", "jsonb"),
		).SetRequired(true),
	toolkit.MustNewParameter("operations", "list of the operations", new([]Operation), nil).
		SetRequired(true),
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
	columnName string
	operations []Operation
}

func NewJsonTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, error) {
	var ops []Operation
	var columnName string

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	p = parameters["operations"]
	if err := p.Scan(&ops); err != nil {
		return nil, fmt.Errorf("unable to parse operations param: %w", err)
	}

	return &JsonTransformer{
		columnName: columnName,
		operations: ops,
	}, nil
}

func (jt *JsonTransformer) Init(ctx context.Context) error {
	return nil
}

func (jt *JsonTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}

func (jt *JsonTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var err error
	var jsonRawValue string
	if err := r.ScanAttribute(jt.columnName, &jsonRawValue); err != nil {
		return nil, fmt.Errorf("cannot scan column value: %w", err)
	}

	if jsonRawValue == toolkit.DefaultNullSeq {
		return r, nil
	}

	for _, op := range jt.operations {
		jsonRawValue, err = op.Apply(jsonRawValue)
		if err != nil {
			return nil, fmt.Errorf("cannot apply operation to the json value: %s: %s: %s", op.Operation, op.Path, op.Value)
		}
	}

	if err = r.SetAttribute(jt.columnName, jsonRawValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(JsonTransformerDefinition)
}
