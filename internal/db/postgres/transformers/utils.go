package transformers

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
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

const (
	DefaultNullSeq      = `\N`
	DefaultNullFraction = 0.3
)

var (
	validate            = validator.New()
	translators         ut.Translator
	enLocales           = en.New()
	universalTranslator = ut.New(enLocales, enLocales)
)

var (
	TransformerMap = map[string]TransformerMeta{
		ReplaceTransformerName:       ReplaceTransformerMeta,
		RegexpReplaceTransformerName: RegexpReplaceTransformerMeta,
		RandomUuidTransformerName:    RandomUuidTransformerMeta,
		SetNullTransformerName:       SetNullTransformerMeta,
		RandomDateTransformerName:    RandomDateTransformerMeta,
		RandomIntTransformerName:     RandomIntTransformerMeta,
		RandomFloatTransformerName:   RandomFloatTransformerMeta,
		RandomStringTransformerName:  RandomStringTransformerMeta,
		RandomBoolTransformerName:    RandomBoolTransformerMeta,
		NoiseDateTransformerName:     NoiseDateTransformerMeta,
		NoiseIntTransformerName:      NoiseIntTransformerMeta,
		NoiseFloatTransformerName:    NoiseFloatTransformerMeta,
		JsonTransformerName:          JsonTransformerMeta,
		MaskingTransformerName:       MaskingTransformerMeta,
		HashTransformerName:          HashTransformerMeta,
	}
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

	err = validate.RegisterTranslation(
		"oneof",
		translators,
		func(ut ut.Translator) error {
			return ut.Add("oneof", "{0} value out of range", true) // see universal-translator for details
		},
		func(ut ut.Translator, fe validator.FieldError) string {
			t, _ := ut.T("oneof", fe.Field())
			return t
		})
	if err != nil {
		panic(fmt.Sprintf("cannot register translation: %s", err))
	}

}

type TransformerFabricFunction func(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error)

type TransformerMeta struct {
	Description       string
	ParamsDescription map[string]string
	NewTransformer    TransformerFabricFunction
	Settings          *TransformerSettings
}

func (tm *TransformerMeta) InstanceTransformer(
	table *pgDomains.TableMeta,
	typeMap *pgtype.Map,
	params map[string]interface{},
) (domains.Transformer, error) {
	if tm.Settings.TransformationType == domains.AttributeTransformation {
		base, err := NewTransformerBase(table, tm.Settings, params, typeMap, tm.Settings.CastVar)
		if err != nil {
			return nil, fmt.Errorf("cannot build transformer base: %w", err)
		}
		return tm.NewTransformer(base, params)
	}
	return nil, fmt.Errorf("unsupporterd transformer type")
}

func GetPgTypeAndEncodingPlan(typeMap *pgtype.Map, typeOid pgDomains.Oid, castVal any) (*pgtype.Type, pgtype.EncodePlan, error) {
	t, ok := typeMap.TypeForOID(uint32(typeOid))
	if !ok {
		return nil, nil, fmt.Errorf("cannot match pgtype %d", typeOid)
	}

	plan := typeMap.PlanEncode(t.OID, pgx.TextFormatCode, castVal)
	if plan == nil {
		return nil, nil, fmt.Errorf("cannot find encoding plan for oid %d", t.OID)
	}
	return t, plan, nil
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

type TransformerSettings struct {
	Name               string                     `json:"name,omitempty"`
	Nullable           bool                       `json:"nullable,omitempty"`
	Variadic           bool                       `json:"variadic,omitempty"`
	Unique             bool                       `json:"unique,omitempty"`
	MaxLength          int64                      `json:"maxLength,omitempty"`
	TransformationType domains.TransformationType `json:"transformationType,omitempty"`
	// Custom transformer settings
	Validate  bool `json:"validate,omitempty"`
	Proto     bool `json:"proto,omitempty"`
	Streaming bool `json:"streaming,omitempty"`

	// SupportedOids - list of the supported pg type oids. - will be replaced SupportedTypes instead
	// Deprecated
	SupportedOids  []int
	SupportedTypes []string
	CastVar        interface{}
	IsCustom       bool
}

func NewTransformerSettings() *TransformerSettings {
	return &TransformerSettings{
		TransformationType: domains.AttributeTransformation,
	}
}

func (tbs *TransformerSettings) SetVariadic() *TransformerSettings {
	tbs.Variadic = true
	return tbs
}

func (tbs *TransformerSettings) SetNullable() *TransformerSettings {
	tbs.Nullable = true
	return tbs
}

func (tbs *TransformerSettings) SetUnique() *TransformerSettings {
	tbs.Unique = true
	return tbs
}

func (tbs *TransformerSettings) SetMaxLength(length int64) *TransformerSettings {
	tbs.MaxLength = length
	return tbs
}

func (tbs *TransformerSettings) SetSupportedOids(oids ...int) *TransformerSettings {
	tbs.SupportedOids = oids
	return tbs
}

func (tbs *TransformerSettings) SetCastVar(castVar interface{}) *TransformerSettings {
	tbs.CastVar = castVar
	return tbs
}

func (tbs *TransformerSettings) SetTransformationType(tt domains.TransformationType) *TransformerSettings {
	tbs.TransformationType = tt
	return tbs
}

func (tbs *TransformerSettings) SetName(name string) *TransformerSettings {
	tbs.Name = name
	return tbs
}

func scan(src any, dest interface{}) error {
	if reflect.ValueOf(dest).Kind() == reflect.Ptr {
		destType := reflect.Indirect(reflect.ValueOf(dest)).Type()
		valType := reflect.TypeOf(src)
		if destType != valType &&
			(!strings.Contains(destType.Name(), "int") && !strings.Contains(valType.Name(), "int")) &&
			(!strings.Contains(destType.Name(), "float") && !strings.Contains(valType.Name(), "float")) {
			return fmt.Errorf("unpexpected types")
		}
	} else {
		return fmt.Errorf("expected pointer")
	}

	switch destTyped := dest.(type) {
	case *time.Time:
		valTyped, ok := src.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time value")
		}
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&valTyped).Elem())
	case *int64:
		var castVar int64
		switch v := src.(type) {
		case int16:
			castVar = int64(v)
		case int32:
			castVar = int64(v)
		case int64:
			castVar = v
		default:
			return fmt.Errorf("expected int64 value")
		}
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&castVar).Elem())
	case *float64:
		var castVar float64
		switch v := src.(type) {
		case float32:
			castVar = float64(v)
		case float64:
			castVar = v
		default:
			return fmt.Errorf("expected float64 value")
		}
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&castVar).Elem())
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

	return scan(val, dest)
}

func parseTransformerParams(src map[string]interface{}, dest interface{}) error {
	if err := mapstructure.Decode(src, dest); err != nil {
		return fmt.Errorf("parameters parsing error: %w", err)
	}

	if err := validate.Struct(dest); err != nil {
		switch errs := err.(type) {
		case validator.ValidationErrors:
			var firstErr string
			for _, item := range errs.Translate(translators) {
				if firstErr == "" {
					firstErr = item
				}
				log.Warn().Msg(item)
			}
			return fmt.Errorf("validation error: %s", firstErr)
		default:
			return fmt.Errorf("validation error: %w", err)
		}
	}
	return nil
}

func getColumnValueFromCsvRecord(data []byte, columnNum int) ([]string, string, error) {
	record, err := parseCsvRecord(data)
	if err != nil {
		return nil, "", err
	}
	return record, record[columnNum], nil
}

func updateAttributeAndBuildRecord(data []string, val string, columnNum int) ([]byte, error) {
	data[columnNum] = val
	return buildCsvRecord(data)
}

func parseCsvRecord(data []byte) ([]string, error) {
	lineReader := csv.NewReader(bytes.NewReader(data))
	lineReader.Comma = '\t'
	values, err := lineReader.Read()
	if err != nil {
		return nil, fmt.Errorf("cannot read dump line: %w", err)
	}
	return values, nil
}

func buildCsvRecord(data []string) ([]byte, error) {
	buf := bytes.Buffer{}
	lineWriter := csv.NewWriter(&buf)
	lineWriter.Comma = '\t'
	if err := lineWriter.Write(data); err != nil {
		return nil, fmt.Errorf("unnable to write line: %w", err)
	}
	lineWriter.Flush()

	res, err := io.ReadAll(&buf)
	if err != nil {
		return nil, fmt.Errorf("cannot read data from tsv reader: %w", err)
	}
	return res, nil
}
