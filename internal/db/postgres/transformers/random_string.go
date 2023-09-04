package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/transformers/utils"
	"github.com/GreenmaskIO/greenmask/internal/domains"
)

const RandomStringTransformerName = "RandomString"

var RandomStringTransformerMeta = utils.TransformerMeta{
	Description: "Generate random string",
	ParamsDescription: map[string]string{
		"minLength": "min length of string. If you want to make string fixes set minLength equally to maxLength",
		"maxLength": "max length of string",
		"symbols":   "the characters range for random string",
	},
	NewTransformer: NewRandomStringTransformer,
	Settings: utils.NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar("").
		SetSupportedOids(
			pgtype.VarcharOID,
			pgtype.TextOID,
		).
		SetName(RandomStringTransformerName),
}

type getRandStringFunc func(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string

type RandomStringTransformerParams struct {
	Symbols  string  `mapstructure:"symbols"`
	Min      int64   `mapstructure:"min" validate:"required"`
	Max      int64   `mapstructure:"max" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomStringTransformer struct {
	utils.TransformerBase
	RandomStringTransformerParams
	symbols  []rune
	buf      []rune
	rand     *rand.Rand
	generate getRandStringFunc
}

func NewRandomStringTransformer(
	base *utils.TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {
	var generate getRandStringFunc = generateFixedString

	tParams := RandomStringTransformerParams{
		Symbols:  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
		Fraction: utils.DefaultNullFraction,
	}

	if err := utils.ParseTransformerParams(params, &tParams); err != nil {
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

func (rst *RandomStringTransformer) TransformAttr(val string) (string, error) {
	if rst.Nullable {
		if rst.rand.Float32() < rst.Fraction {
			return utils.DefaultNullSeq, nil
		}
	}
	return rst.generate(rst.rand, rst.buf, rst.Min, rst.Max, rst.symbols), nil
}

func (rst *RandomStringTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := utils.GetColumnValueFromCsvRecord(rst.Table, data, rst.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := rst.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return utils.UpdateAttributeAndBuildRecord(rst.Table, record, transformedAttr, rst.ColumnNum)
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
