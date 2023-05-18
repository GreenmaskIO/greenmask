package transformers

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type TransformerFabricFunction func(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error)

type TransformerMeta struct {
	Description       string
	ParamsDescription map[string]string
	SupportedTypeOids []int
	NewTransformer    TransformerFabricFunction
}

var (
	TransformerMap = map[string]TransformerMeta{
		"Replace":      ReplaceTransformerMeta,
		"UUID":         UuidTransformerMeta,
		"SetNull":      SetNullTransformerMeta,
		"GoTemplate":   GoTemplateTransformerMata,
		"RandomDate":   RandomDateTransformerMeta,
		"RandomInt":    RandomIntTransformerMeta,
		"RandomFloat":  RandomFloatTransformerMeta,
		"RandomString": RandomStringTransformerMeta,
	}
)

var (
	DateTypes = []int{
		pgtype.DateOID,
		pgtype.TimestampOID,
		pgtype.TimestamptzOID,
	}
	StringTypes = []int{
		pgtype.TextOID,
		pgtype.VarcharOID,
	}
	IntTypes = []int{
		pgtype.Int2OID,
		pgtype.Int4OID,
		pgtype.Int8OID,
	}
	FloatTypes = []int{
		pgtype.Float4OID,
		pgtype.Float8OID,
	}
	UuidTypes = []int{
		pgtype.UUIDOID,
		pgtype.TextOID,
	}
)

func GetPgCodeAndEncodingPlan(typeMap *pgtype.Map, typeOid uint32, castVal any) (*pgtype.Type, pgtype.EncodePlan, error) {
	t, ok := typeMap.TypeForOID(typeOid)
	if !ok {
		return nil, nil, fmt.Errorf("cannot match pgtype %d", typeOid)
	}

	plan := typeMap.PlanEncode(t.OID, pgx.TextFormatCode, castVal)
	if plan == nil {
		return nil, nil, fmt.Errorf("cannot find encoding plan for oid %d", t.OID)
	}
	return t, plan, nil
}

func CastFloat(t *pgtype.Type, typeMap *pgtype.Map, val string) (float64, error) {
	var typeViolationErrStr = "value out of range: value must be from %f to %f"
	var res float64
	decoded, err := t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(val))
	if err != nil {
		return 0, fmt.Errorf("cannot decode maxend value: %w", err)
	}
	switch v := decoded.(type) {
	case float32:
		res = float64(v)
		if math.Abs(res) < math.SmallestNonzeroFloat32 || math.Abs(res) > math.MaxFloat32 {
			return 0, fmt.Errorf(typeViolationErrStr, math.SmallestNonzeroFloat32, math.MaxFloat32)
		}
	case float64:
		res = v
		if math.Abs(res) < math.SmallestNonzeroFloat64 || math.Abs(res) > math.MaxFloat64 {
			return 0, fmt.Errorf(typeViolationErrStr, math.SmallestNonzeroFloat64, math.MaxFloat64)
		}
	default:
		return 0, errors.New("cannot cast string to float type")
	}
	return res, nil
}

func CastInt(t *pgtype.Type, typeMap *pgtype.Map, val string) (int64, error) {
	var res int64
	decoded, err := t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(val))
	if err != nil {
		return 0, err
	}
	switch v := decoded.(type) {
	case int16:
		res = int64(v)
	case int32:
		res = int64(v)
	case int64:
		res = v
	default:
		return 0, errors.New("cannot cast string to int type")
	}
	return res, nil
}

func Round(x, unit float64) float64 {
	return math.Floor(x*unit) / unit
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
