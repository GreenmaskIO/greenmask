package transformers

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var (
	validate            = validator.New()
	translators         ut.Translator
	enLocales           = en.New()
	universalTranslator = ut.New(enLocales, enLocales)
)

func init() {
	var found bool
	translators, found = universalTranslator.GetTranslator("en")
	if !found {
		panic("translation not found")
	}

	err := validate.RegisterTranslation(
		"required",
		translators,
		func(ut ut.Translator) error {
			return ut.Add("required", "expected {0} key", true) // see universal-translator for details
		},
		func(ut ut.Translator, fe validator.FieldError) string {
			t, _ := ut.T("required", fe.Field())
			return t
		})
	if err != nil {
		panic(fmt.Sprintf("cannot register translation: %s", err))
	}
}

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

func GetPgTypeAndEncodingPlan(typeMap *pgtype.Map, typeOid uint32, castVal any) (*pgtype.Type, pgtype.EncodePlan, error) {
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
		panic(fmt.Sprintf(`wrong Truncate value "%s"`, *part))
	}
	return time.Date(year, month, day, hour, minute, second, nano,
		t.Location(),
	)
}

type TransformerBase struct {
	Column     pgDomains.ColumnMeta
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
	TypeMap    *pgtype.Map
}

func NewTransformerBase(column pgDomains.ColumnMeta, typeMap *pgtype.Map) (*TransformerBase, error) {
	if typeMap == nil {
		return nil, fmt.Errorf("typeMap cannot be nil")
	}
	t, plan, err := GetPgTypeAndEncodingPlan(typeMap, column.TypeOid, time.Time{})
	if err != nil {
		return nil, err
	}
	return &TransformerBase{
		Column:     column,
		PgType:     t,
		EncodePlan: plan,
		TypeMap:    typeMap,
	}, nil
}

func (tb *TransformerBase) Transform(val string) (string, error) {
	return "", nil
}

func (tb *TransformerBase) Scan(src string, dest interface{}) error {
	val, err := tb.PgType.Codec.DecodeValue(tb.TypeMap, tb.Column.TypeOid, pgx.TextFormatCode, []byte(src))
	if err != nil {
		return fmt.Errorf("cannot decode value: %w", err)
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
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&valTyped).Elem())
	default:
		return fmt.Errorf("unsopported type")
	}

	return nil
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
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&valTyped).Elem())
	default:
		return fmt.Errorf("unsopported type")
	}

	return nil
}

func parseTransformerParams(src map[string]interface{}, dest interface{}) error {
	if err := mapstructure.Decode(src, dest); err != nil {
		return fmt.Errorf("parameters parsing error: %w", err)
	}

	if err := validate.Struct(dest); err != nil {
		errs, ok := err.(validator.ValidationErrors)
		if ok {
			var firstErr string
			for _, item := range errs.Translate(translators) {
				if firstErr == "" {
					firstErr = item
				}
				log.Warn().Msg(item)
			}
			return fmt.Errorf("validation error: %s", firstErr)
		}
		return fmt.Errorf("validation error: %w", err)
	}
	return nil
}
