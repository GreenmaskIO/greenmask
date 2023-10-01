package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var RandomStringTransformerDefinition = toolkit.NewDefinition(
	toolkit.NewTransformerProperties(
		"RandomString",
		"Generate random string",
	),

	NewRandomStringTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"min_length",
		"min length of string",
		new(int64),
		nil,
	).SetRequired(true),

	toolkit.MustNewParameter(
		"max_length",
		"max length of string",
		new(int64),
		nil,
	).SetRequired(true),

	toolkit.MustNewParameter(
		"symbols",
		"the characters range for random string",
		new(string),
		New("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"),
	),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
		new(bool),
		New(false),
	),
)

type getRandStringFunc func(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string

type RandomStringTransformer struct {
	columnName string
	keepNull   bool
	min        int64
	max        int64
	symbols    []rune
	buf        []rune
	rand       *rand.Rand
	generate   getRandStringFunc
}

func NewRandomStringTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var generator getRandStringFunc = generateFixedString
	var columnName, symbols string
	var minLength, maxLength int64
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

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
		columnName: columnName,
		keepNull:   keepNull,
		min:        minLength,
		max:        maxLength,
		symbols:    []rune(symbols),
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
		buf:        make([]rune, maxLength),
		generate:   generator,
	}, nil, nil
}

func (rst *RandomStringTransformer) Init(ctx context.Context) error {
	return nil
}

func (rst *RandomStringTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetAttribute(rst.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rst.keepNull {
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
