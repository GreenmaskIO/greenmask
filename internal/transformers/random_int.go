package transformers

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/slices"

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
		pgtype.TextOID,
		pgtype.VarcharOID,
	},
	NewTransformer: NewRandomIntTransformer,
}

type RandomIntTransformer struct {
	Column     pgDomains.ColumnMeta
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
	min        int64
	max        int64
	rand       *rand.Rand
}

func NewRandomIntTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var minInt, maxInt int64
	var castVar int64
	var useType = "int8"
	var useOid = column.TypeOid
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

	if !slices.Contains(IntTypes, int(column.TypeOid)) {
		if slices.Contains(StringTypes, int(column.TypeOid)) {
			castVar = int64(0)
			adaptedType, ok := typeMap.TypeForName(useType)
			if !ok {
				return nil, fmt.Errorf("unsupporter int type %s", useType)
			}
			useOid = adaptedType.OID
		} else {
			return nil, fmt.Errorf("unsupported type oid %d", column.TypeOid)
		}
	}

	t, plan, err := GetPgTypeAndEncodingPlan(typeMap, useOid, castVar)
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

	return &RandomIntTransformer{
		Column:     column,
		PgType:     t,
		EncodePlan: plan,
		min:        minInt,
		max:        maxInt,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil
}

func (gtt *RandomIntTransformer) Transform(val string) (string, error) {
	resInt := gtt.rand.Int63n(gtt.max-gtt.min) + gtt.min
	res, err := gtt.EncodePlan.Encode(resInt, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
