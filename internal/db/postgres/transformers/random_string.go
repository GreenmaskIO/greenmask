package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RandomStringTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomString",
		"Generate random string",
	),

	NewRandomStringTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"min_length",
		"min length of string",
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"max_length",
		"max length of string",
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"symbols",
		"the characters range for random string",
	).SetDefaultValue(toolkit2.ParamsValue("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")),

	toolkit2.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
	).SetDefaultValue(toolkit2.ParamsValue("true")),
)

type getRandStringFunc func(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string

type RandomStringTransformer struct {
	columnName      string
	keepNull        bool
	min             int64
	max             int64
	symbols         []rune
	buf             []rune
	rand            *rand.Rand
	generate        getRandStringFunc
	affectedColumns map[int]string
}

func NewRandomStringTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var generator getRandStringFunc = generateFixedString
	var columnName, symbols string
	var minLength, maxLength int64
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["min_length"]
	if err := p.Scan(&minLength); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "min_length" param: %w`, err)
	}

	p = parameters["max_length"]
	if err := p.Scan(&maxLength); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "max_length" param: %w`, err)
	}

	p = parameters["symbols"]
	if err := p.Scan(&symbols); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "symbols" param: %w`, err)
	}

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	if minLength != maxLength {
		generator = generateVariadicString
	}

	return &RandomStringTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		min:             minLength,
		max:             maxLength,
		symbols:         []rune(symbols),
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		buf:             make([]rune, maxLength),
		generate:        generator,
		affectedColumns: affectedColumns,
	}, nil, nil
}

func (rst *RandomStringTransformer) GetAffectedColumns() map[int]string {
	return rst.affectedColumns
}

func (rst *RandomStringTransformer) Init(ctx context.Context) error {
	return nil
}

func (rst *RandomStringTransformer) Done(ctx context.Context) error {
	return nil
}

func (rst *RandomStringTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	val, err := r.GetRawAttributeValueByName(rst.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rst.keepNull {
		return r, nil
	}

	res := rst.generate(rst.rand, rst.buf, rst.min, rst.max, rst.symbols)
	if err := r.SetAttribute(rst.columnName, &res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func generateFixedString(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string {
	for i := int64(0); i < maxLength; i++ {
		buf[i] = symbols[rand.Int63n(maxLength)]
	}
	return string(buf)
}

func generateVariadicString(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string {
	length := (minLength) + r.Int63n(maxLength-minLength)
	for i := int64(0); i < length; i++ {
		buf[i] = symbols[r.Int63n(maxLength)]
	}
	return string(buf[:length])
}
