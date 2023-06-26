package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/tidwall/sjson"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var JsonTransformerSupportedOids = []int{
	pgtype.JSONOID,
	pgtype.JSONBOID,
}

var JsonTransformerMeta = TransformerMeta{
	Description: `Json with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"operations": "json changing operations",
	},
	SupportedTypeOids: JsonTransformerSupportedOids,
	NewTransformer:    NewJsonTransformer,
}

type JsonTransformerParams struct {
	Operations []Operation `mapstructure:"operations"`
	Nullable   bool        `mapstructure:"nullable"`
	Fraction   float32     `mapstructure:"fraction"`
}

type Operation struct {
	Operation string `mapstructure:"operation" validate:"required, oneof=delete set"`
	//Type      string      `mapstructure:"type,omitempty" validate:"required, oneof=nil bool string int float "`
	Value interface{} `mapstructure:"value,omitempty"`
	Path  string      `mapstructure:"path" validate:"required"`
}

func (oo *Operation) Apply(inp string) (string, error) {
	var val string
	var err error
	if oo.Operation == "set" {
		val, err = sjson.Set(inp, oo.Path, oo.Value)
	} else {
		val, err = sjson.Delete(inp, oo.Path)
	}
	if err != nil {
		return "", fmt.Errorf("cannot %s value: %w", oo.Operation, err)
	}
	return val, nil
}

type JsonTransformer struct {
	TransformerBase
	JsonTransformerParams
	Column pgDomains.ColumnMeta
	rand   *rand.Rand
}

func NewJsonTransformer(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(column, typeMap, useType, JsonTransformerSupportedOids, "")
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := JsonTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	res := &JsonTransformer{
		TransformerBase:       *base,
		JsonTransformerParams: tParams,
		Column:                column,
		rand:                  rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return res, nil
}

func (rt *JsonTransformer) Transform(val string) (string, error) {
	var err error
	if val == DefaultNullSeq {
		return val, nil
	}
	if rt.Nullable {
		if rt.rand.Float32() < rt.Fraction {
			return DefaultNullSeq, nil
		}
	}

	for _, op := range rt.Operations {
		val, err = op.Apply(val)
		if err != nil {
			return "", fmt.Errorf("cannot apply operation to the json value: %s: %s: %s", op.Operation, op.Path, op.Value)
		}
	}

	return val, nil
}
