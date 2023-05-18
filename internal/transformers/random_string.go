package transformers

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type getRandStringFunc func(r *rand.Rand, buf []rune, minLength, maxLength int64, symbols []rune) string

var RandomStringTransformerMeta = TransformerMeta{
	Description: "Generate random string",
	ParamsDescription: map[string]string{
		"minLength": "min length of string. If you want to make string fixes set minLength equally to maxLength",
		"maxLength": "max length of string",
		"symbols":   "the characters range for random string",
	},
	SupportedTypeOids: []int{
		pgtype.VarcharOID,
		pgtype.TextOID,
	},
	NewTransformer: NewRandomStringTransformer,
}

type RandomStringTransformer struct {
	Column    pgDomains.ColumnMeta
	symbols   []rune
	minLength int64
	maxLength int64
	buf       []rune
	rand      *rand.Rand
	generate  getRandStringFunc
}

func NewRandomStringTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var generate getRandStringFunc = generateFixedString
	var symbols = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if typeMap == nil {
		return nil, errors.New("typeMap cannot be nil")
	}
	s, ok := params["symbols"]
	if ok {
		symbols = []rune(s)
	}

	minLengthStr, ok := params["minLength"]
	if !ok {
		return nil, errors.New("expected length key")
	}
	minLength, err := strconv.ParseInt(minLengthStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cannot cast minLengthStr value to uint64")
	}

	maxLengthStr, ok := params["minLength"]
	if !ok {
		return nil, errors.New("expected length key")
	}
	maxLength, err := strconv.ParseInt(maxLengthStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cannot cast minLengthStr value to uint64")
	}

	if maxLength < minLength {
		return nil, fmt.Errorf("maxLength cannot be less than minLength")
	}

	if maxLength > minLength {
		generate = generateFloatedString
	}

	return &RandomStringTransformer{
		Column:    column,
		rand:      rand.New(rand.NewSource(time.Now().UnixMicro())),
		buf:       make([]rune, maxLength),
		symbols:   symbols,
		minLength: minLength,
		maxLength: maxLength,
		generate:  generate,
	}, nil
}

func (gtt *RandomStringTransformer) Transform(val string) (string, error) {
	return gtt.generate(gtt.rand, gtt.buf, gtt.minLength, gtt.maxLength, gtt.symbols), nil
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
