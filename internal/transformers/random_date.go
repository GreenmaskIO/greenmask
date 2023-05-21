package transformers

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mitchellh/mapstructure"
	"golang.org/x/exp/slices"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Test this transformer

var RandomDateTransformerMeta = TransformerMeta{
	Description: "Generate random date",
	ParamsDescription: map[string]string{
		"min":      "min value",
		"max":      "max value",
		"truncate": "Truncate date till the part (year, month, day, hour, second, nano)",
		"useType":  "use another type instead default timestamp for textual type (date, timestamp, timestamptz)",
		"nullable": "generate null value randomly (default false)",
		"fraction": "NULL value distribution within the table (default Fraction 10%)",
	},
	SupportedTypeOids: []int{
		pgtype.DateOID,
		pgtype.TimestampOID,
		pgtype.TimestamptzOID,
		pgtype.TextOID,
		pgtype.VarcharOID,
	},
	NewTransformer: NewRandomDateTransformer,
}

type dateGeneratorFunc func(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time

type RandomDateTransformer struct {
	column     pgDomains.ColumnMeta
	useOid     uint32
	pgType     *pgtype.Type
	encodePlan pgtype.EncodePlan
	rand       *rand.Rand
	generate   dateGeneratorFunc
	StartDate  time.Time `mapstructure:"min" greenmask:"usepgtype"`
	EndDate    time.Time `mapstructure:"max" greenmask:"usepgtype"`
	UseType    string    `mapstructure:"useType"`
	Truncate   string    `mapstructure:"truncate"`
	Nullable   bool      `mapstructure:"nullable"`
	Fraction   float32   `mapstructure:"fraction"`
}

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

func ToPgTypeHookFunc(cast interface{}, codec pgtype.Codec, typeMap *pgtype.Map, oid uint32) mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if t != reflect.TypeOf(cast) {
			return data, nil
		}

		switch f.Kind() {
		case reflect.String:
			//return time.Parse(time.RFC3339, data.(string))
			val, err := codec.DecodeDatabaseSQLValue(typeMap, oid, pgx.TextFormatCode, []byte(data.(string)))
			if err != nil {
				return nil, fmt.Errorf("cannot decode max value: %w", err)
			}
			// TODO: Add type cast and val type validation that they are equal
			return val, nil
		default:
			return nil, fmt.Errorf("unsupported type")
		}
	}
}

func NewRandomDateTransformerV2(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	//useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	var castVar time.Time
	var res = &RandomDateTransformer{}
	var useOid = column.TypeOid

	t, plan, err := GetPgTypeAndEncodingPlan(typeMap, useOid, castVar)
	if err != nil {
		return nil, err
	}
	res.encodePlan = plan

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata:   nil,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(ToPgTypeHookFunc(castVar, t.Codec, typeMap, useOid)),
		Result:     res,
	})
	if err := decoder.Decode(params); err != nil {
		return nil, err
	}

	return res, nil
}

func NewRandomDateTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var castVar any
	var useType = "timestamp"
	var truncate = ""
	var fraction = float32(0.1)
	var useOid = column.TypeOid
	var startDate, endDate time.Time
	var generator dateGeneratorFunc = generateRandomTime

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
	ut, ok := params["useType"]
	if ok {
		if !slices.Contains([]string{"date", "timestamp", "timestamptz"}, ut) {
			return nil, fmt.Errorf("unsupported type %s", ut)
		}
		useType = ut
	}
	tp, ok := params["truncate"]
	if ok {
		if !slices.Contains(truncateParts, tp) {
			return nil, fmt.Errorf(`wrong Truncate value "%s"`, tp)
		}
		truncate = tp
		generator = generateRandomTimeTruncate
	}

	if slices.Contains(DateTypes, int(column.TypeOid)) {
		castVar = time.Time{}
	} else if slices.Contains(StringTypes, int(column.TypeOid)) {
		castVar = time.Time{}
		adaptedType, ok := typeMap.TypeForName(useType)
		if !ok {
			return nil, fmt.Errorf("unsupporter date type %s", useType)
		}
		useOid = adaptedType.OID
	} else {
		return nil, fmt.Errorf("unsupported type oid %d", column.TypeOid)
	}

	t, plan, err := GetPgTypeAndEncodingPlan(typeMap, useOid, castVar)
	if err != nil {
		return nil, err
	}

	val, err := t.Codec.DecodeValue(typeMap, useOid, pgx.TextFormatCode, []byte(start))
	if err != nil {
		return nil, fmt.Errorf("cannot decode min value: %w", err)
	}
	switch v := val.(type) {
	case time.Time:
		startDate = v
	default:
		return nil, errors.New("cannot cast type of min key")
	}

	val, err = t.Codec.DecodeValue(typeMap, useOid, pgx.TextFormatCode, []byte(end))
	if err != nil {
		return nil, fmt.Errorf("cannot decode max value: %w", err)
	}
	switch v := val.(type) {
	case time.Time:
		endDate = v
	default:
		return nil, fmt.Errorf("cannot cast type of max key: unexpected type %+v", v)
	}

	return &RandomDateTransformer{
		column:     column,
		useOid:     useOid,
		pgType:     t,
		encodePlan: plan,
		StartDate:  startDate,
		EndDate:    endDate,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
		generate:   generator,
		Truncate:   truncate,
		Fraction:   fraction,
	}, nil
}

func (gtt *RandomDateTransformer) Transform(val string) (string, error) {
	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return defaultNullSeq, nil
		}
	}
	resTime := gtt.generate(gtt.rand, &gtt.StartDate, &gtt.EndDate, &gtt.Truncate)
	res, err := gtt.encodePlan.Encode(resTime, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func generateRandomTime(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time {
	delta := endDate.UnixMicro() - startDate.UnixMicro()
	return time.UnixMicro(r.Int63n(delta) + startDate.UnixMicro())
}

func generateRandomTimeTruncate(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time {
	delta := endDate.UnixMicro() - startDate.UnixMicro()
	randVal := time.UnixMicro(r.Int63n(delta) + startDate.UnixMicro())
	return truncateDate(&randVal, truncate)
}
