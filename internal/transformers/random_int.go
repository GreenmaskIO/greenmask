package transformers

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var RandomIntTransformerMeta = TransformerMeta{
	Description: "Generate random int",
	ParamsDescription: map[string]string{
		"min": "min value",
		"max": "max value",
	},
	SupportedTypeOids: []int{
		pgtype.Int2OID,
		pgtype.Int4OID,
		pgtype.Int8OID,
		//pgtype.TextOID,
		//pgtype.VarcharOID,
		//pgtype.NumericOID,
	},
	NewTransformer: NewRandomIntTransformer,
}

type RandomIntTransformer struct {
	Column     pgDomains.ColumnMeta
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
	min        int64
	max        int64
	delta      int64
	rand       *rand.Rand
}

func NewRandomIntTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var minInt, maxInt int64
	if typeMap == nil {
		return nil, errors.New("typeMap cannot be nil")
	}
	start, ok := params["min"]
	if !ok {
		return nil, errors.New("expected min key")
	}
	if start == "" {
		return nil, errors.New("min key cannot be empty string")
	}
	end, ok := params["max"]
	if !ok {
		return nil, errors.New("expected max key")
	}
	if end == "" {
		return nil, errors.New("max key cannot be empty string")
	}

	t, plan, err := GetPgCodeAndEncodingPlan(typeMap, column.TypeOid, minInt)
	if err != nil {
		return nil, err
	}

	minInt, err = CastInt(t, typeMap, start)
	if err != nil {
		return nil, fmt.Errorf("cannot cast min value: %w", err)
	}

	maxInt, err = CastInt(t, typeMap, end)
	if err != nil {
		return nil, fmt.Errorf("cannot cast max value: %w", err)
	}
	delta := maxInt - minInt

	return &RandomIntTransformer{
		Column:     column,
		PgType:     t,
		EncodePlan: plan,
		min:        minInt,
		max:        maxInt,
		delta:      delta,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil
}

func (gtt *RandomIntTransformer) Transform(val string) (string, error) {
	resInt := gtt.rand.Int63n(gtt.delta) + gtt.min
	res, err := gtt.EncodePlan.Encode(resInt, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
