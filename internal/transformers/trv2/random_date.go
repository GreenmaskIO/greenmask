package trv2

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mitchellh/mapstructure"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
	"github.com/wwoytenko/greenfuscator/internal/transformers"
)

// TODO: Test this transformer

type dateGeneratorFunc func(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time

type RandomDateTransformerParams struct {
	Min      string  `mapstructure:"min"`
	Max      string  `mapstructure:"max"`
	UseType  string  `mapstructure:"useType"`
	Truncate string  `mapstructure:"truncate"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomDateTransformer struct {
	TransformerBase
	RandomDateTransformerParams `mapstructure:",squash"`
	rand                        *rand.Rand
	generate                    dateGeneratorFunc
	min                         time.Time
	max                         time.Time
}

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

type TransformerBase struct {
	Column     pgDomains.ColumnMeta
	UseOid     uint32
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
	TypeMap    *pgtype.Map
}

func Scan(src string, dest interface{}, oid uint32, typeMap *pgtype.Map, pgType *pgtype.Type) error {
	val, err := pgType.Codec.DecodeValue(typeMap, oid, pgx.TextFormatCode, []byte(src))
	if err != nil {
		return fmt.Errorf("cannot decode min value: %w", err)
	}

	if reflect.ValueOf(dest).Kind() == reflect.Ptr {
		destType := reflect.Indirect(reflect.ValueOf(dest)).Type()
		valType := reflect.TypeOf(val)
		if destType != valType {
			return fmt.Errorf("unpexpected types")
		}
	} else {
		return fmt.Errorf("expected pointer")
	}

	switch destTyped := dest.(type) {
	case *time.Time:
		valTyped, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time value")
		}
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(valTyped).Elem())
	default:
		return fmt.Errorf("unsopported type")
	}

	return nil
}

func NewRandomDateTransformerV2(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	//useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	var useOid = column.TypeOid

	t, plan, err := transformers.GetPgTypeAndEncodingPlan(typeMap, useOid, time.Time{})
	if err != nil {
		return nil, err
	}

	res := &RandomDateTransformer{
		TransformerBase: TransformerBase{
			Column:     column,
			UseOid:     useOid,
			PgType:     t,
			EncodePlan: plan,
			TypeMap:    typeMap,
		},
		rand: rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	if err = mapstructure.Decode(params, res); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if err = Scan(res.Min, &res.min, useOid, typeMap, t); err != nil {
		return nil, fmt.Errorf("cannot parse min param")
	}

	return res, nil
}

func (gtt *RandomDateTransformer) Transform(val string) (string, error) {
	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return transformers.DefaultNullSeq, nil
		}
	}
	resTime := gtt.generate(gtt.rand, &gtt.min, &gtt.max, &gtt.Truncate)
	res, err := gtt.EncodePlan.Encode(resTime, nil)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func generateRandomTime(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time {
	delta := endDate.UnixMicro() - startDate.UnixMicro()
	return time.UnixMicro(r.Int63n(delta) + startDate.UnixMicro())
}

//func generateRandomTimeTruncate(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time {
//	delta := endDate.UnixMicro() - startDate.UnixMicro()
//	randVal := time.UnixMicro(r.Int63n(delta) + startDate.UnixMicro())
//	return truncateDate(&randVal, truncate)
//}
