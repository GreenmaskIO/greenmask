package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/tidwall/sjson"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	JsonTransformerName = "Json"
)

var JsonTransformerMeta = TransformerMeta{
	Description: `Json with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"operations": "json changing operations",
	},
	NewTransformer: NewJsonTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar("").
		SetSupportedOids(
			pgtype.JSONOID,
			pgtype.JSONBOID,
		).
		SetName(JsonTransformerName),
}

type JsonTransformerParams struct {
	Operations []Operation `mapstructure:"operations"`
}

type Operation struct {
	Operation string `mapstructure:"operation" validate:"required, oneof=delete set"`
	//TypeName      string      `mapstructure:"type,omitempty" validate:"required, oneof=nil bool string int float "`
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
	rand *rand.Rand
}

func NewJsonTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := JsonTransformerParams{}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	res := &JsonTransformer{
		TransformerBase:       *base,
		JsonTransformerParams: tParams,
		rand:                  rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	return res, nil
}

func (jt *JsonTransformer) TransformAttr(val string) (string, error) {
	var err error
	if val == DefaultNullSeq {
		return val, nil
	}
	if jt.Nullable {
		if jt.rand.Float32() < jt.Fraction {
			return DefaultNullSeq, nil
		}
	}

	for _, op := range jt.Operations {
		val, err = op.Apply(val)
		if err != nil {
			return "", fmt.Errorf("cannot apply operation to the json value: %s: %s: %s", op.Operation, op.Path, op.Value)
		}
	}

	return val, nil
}

func (jt *JsonTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(data, jt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := jt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(record, transformedAttr, jt.ColumnNum)
}
