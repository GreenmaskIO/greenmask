package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type getRandStringFunc func(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string

var RandomStringTransformerSupportedOids = []int{
	pgtype.VarcharOID,
	pgtype.TextOID,
}

var RandomStringTransformerMeta = TransformerMeta{
	Description: "Generate random string",
	ParamsDescription: map[string]string{
		"minLength": "min length of string. If you want to make string fixes set minLength equally to maxLength",
		"maxLength": "max length of string",
		"symbols":   "the characters range for random string",
	},
	SupportedTypeOids: RandomStringTransformerSupportedOids,
	NewTransformer:    NewRandomStringTransformer,
}

type RandomStringTransformerParams struct {
	Symbols  string  `mapstructure:"symbols"`
	Min      int64   `mapstructure:"min" validate:"required"`
	Max      int64   `mapstructure:"max" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomStringTransformer struct {
	TransformerBase
	RandomStringTransformerParams
	symbols  []rune
	buf      []rune
	rand     *rand.Rand
	generate getRandStringFunc
}

func NewRandomStringTransformer(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	var generate getRandStringFunc = generateFixedString

	base, err := NewTransformerBase(column, typeMap, useType, RandomStringTransformerSupportedOids, "")
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := RandomStringTransformerParams{
		Symbols:  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
		Fraction: DefaultNullFraction,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Max != tParams.Min {
		generate = generateFloatedString
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return &RandomStringTransformer{
		TransformerBase:               *base,
		RandomStringTransformerParams: tParams,
		rand:                          rand.New(rand.NewSource(time.Now().UnixMicro())),
		buf:                           make([]rune, tParams.Max),
		generate:                      generate,
		symbols:                       []rune(tParams.Symbols),
	}, nil
}

func (gtt *RandomStringTransformer) Transform(val string) (string, error) {
	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return gtt.generate(gtt.rand, gtt.buf, gtt.Min, gtt.Max, gtt.symbols), nil
}

func generateFixedString(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string {
	for i := int64(0); i < maxLength; i++ {
		buf[i] = symbols[rand.Int63n(maxLength)]
	}
	return string(buf)
}

func generateFloatedString(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string {
	length := (minLength) + r.Int63n(maxLength-minLength)
	for i := int64(0); i < length; i++ {
		buf[i] = symbols[r.Int63n(maxLength)]
	}
	return string(buf[:length])
}
