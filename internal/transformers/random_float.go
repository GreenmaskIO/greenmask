package transformers

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/slices"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var RandomFloatTransformerMeta = TransformerMeta{
	Description: "Generate random float",
	ParamsDescription: map[string]string{
		"min":       "min value",
		"max":       "max value",
		"precision": "precision of the random value",
	},
	SupportedTypeOids: []int{
		pgtype.Float4OID,
		pgtype.Float8ArrayOID,
		pgtype.VarcharOID,
		pgtype.TextOID,
	},
	NewTransformer: NewRandomFloatTransformer,
}

type RandomFloatTransformer struct {
	Column     pgDomains.ColumnMeta
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
	min        float64
	max        float64
	precision  float64
	rand       *rand.Rand
}

func NewRandomFloatTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var castVar float64
	var useType = "float8"
	var useOid = column.TypeOid
	var precisionSize int64 = 4
	var minFloat, maxFloat float64
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
		return nil, errors.New("end key cannot be empty string")
	}
	ut, ok := params["useType"]
	if ok {
		useType = ut
	}
	if !slices.Contains(FloatTypes, int(column.TypeOid)) {
		if slices.Contains(StringTypes, int(column.TypeOid)) {
			adaptedType, ok := typeMap.TypeForName(useType)
			if !ok {
				return nil, fmt.Errorf("unsupporter date type %s", useType)
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

	minFloat, err = CastFloat(t, typeMap, start)
	if err != nil {
		return nil, fmt.Errorf("cannot cast min value: %w", err)
	}

	maxFloat, err = CastFloat(t, typeMap, end)
	if err != nil {
		return nil, fmt.Errorf("cannot cast maxend value: %w", err)
	}

	precision, ok := params["precision"]
	if ok {
		precisionSize, err = strconv.ParseInt(precision, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot cast precision value to int")
		}
	}
	precisionFloat := math.Pow(10, float64(precisionSize))

	return &RandomFloatTransformer{
		Column:     column,
		PgType:     t,
		EncodePlan: plan,
		min:        minFloat,
		max:        maxFloat,
		precision:  precisionFloat,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil
}

func (gtt *RandomFloatTransformer) Transform(val string) (string, error) {
	resFloat := gtt.min + gtt.rand.Float64()*(gtt.max-gtt.min)
	resFloat = Round(resFloat, gtt.precision)
	res, err := gtt.EncodePlan.Encode(resFloat, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
