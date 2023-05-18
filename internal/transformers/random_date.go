package transformers

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
		"truncate": "truncate date till the part (year, month, day, hour, second, nano)",
		"useType":  "useful for text value when date type determination impossible. Default timestamp. (date, timestamp, timestamptz)",
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
	Column     pgDomains.ColumnMeta
	useOid     uint32
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
	startDate  time.Time
	endDate    time.Time
	truncate   string
	rand       *rand.Rand
	generate   dateGeneratorFunc
}

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

func NewRandomDateTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var castVar any
	var useType = "timestamp"
	var truncate = ""
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
		useType = ut
	}
	tp, ok := params["truncate"]
	if ok {
		if !slices.Contains(truncateParts, tp) {
			return nil, fmt.Errorf(`wrong truncate value "%s"`, tp)
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

	t, plan, err := GetPgCodeAndEncodingPlan(typeMap, useOid, castVar)
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
		Column:     column,
		useOid:     useOid,
		PgType:     t,
		EncodePlan: plan,
		startDate:  startDate,
		endDate:    endDate,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
		generate:   generator,
		truncate:   truncate,
	}, nil
}

func (gtt *RandomDateTransformer) Transform(val string) (string, error) {
	resTime := gtt.generate(gtt.rand, &gtt.startDate, &gtt.endDate, &gtt.truncate)
	res, err := gtt.EncodePlan.Encode(resTime, nil)
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

// TODO: You should optimize this function or find another way to implement
func truncateDate(t *time.Time, part *string) time.Time {
	// year, month, day, hour, minute, second, nano
	var month time.Month = 1
	var day = 1
	var year, hour, minute, second, nano int
	switch *part {
	case "nano":
		nano = t.Nanosecond()
		fallthrough
	case "second":
		second = t.Second()
		fallthrough
	case "minute":
		minute = t.Minute()
		fallthrough
	case "hour":
		hour = t.Hour()
		fallthrough
	case "day":
		day = t.Day()
		fallthrough
	case "month":
		month = t.Month()
		fallthrough
	case "year":
		year = t.Year()
	default:
		panic(fmt.Sprintf(`wrong truncate value "%s"`, *part))
	}
	return time.Date(year, month, day, hour, minute, second, nano,
		t.Location(),
	)
}
