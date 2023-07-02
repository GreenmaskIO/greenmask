package transformers

import (
	"errors"
	"fmt"
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
	"golang.org/x/exp/slices"

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
	column *pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(table, column, tm.Settings, params, typeMap, tm.Settings.CastVar)
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}
	return tm.NewTransformer(base, params)
}

var (
	TransformerMap = map[string]TransformerMeta{
		"Replace":       ReplaceTransformerMeta,
		"RegexpReplace": RegexpReplaceTransformerMeta,
		"UUID":          UuidTransformerMeta,
		"SetNull":       SetNullTransformerMeta,
		"RandomDate":    RandomDateTransformerMeta,
		"RandomInt":     RandomIntTransformerMeta,
		"RandomFloat":   RandomFloatTransformerMeta,
		"RandomString":  RandomStringTransformerMeta,
		"RandomBool":    RandomBoolTransformerMeta,
		"NoiseDate":     NoiseDateTransformerMeta,
		"NoiseInt":      NoiseIntTransformerMeta,
		"NoiseFloat":    NoiseFloatTransformerMeta,
		"JsonFloat":     JsonTransformerMeta,
		"Masking":       MaskingTransformerMeta,
	}
)

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
	Nullable      bool
	Variadic      bool
	Unique        bool
	MaxLength     int64
	SupportedOids []int
	CastVar       interface{}
}

func NewTransformerSettings() *TransformerSettings {
	return &TransformerSettings{}
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

const (
	FatalErrorSeverity   = "fatal"
	WarningErrorSeverity = "warning"

	FkConstraintType         = "ForeignKey"
	CheckConstraintType      = "Check"
	NotNullConstraintType    = "Check"
	PkConstraintType         = "PrimaryKey"
	UniqueConstraintType     = "Unique"
	ReferencesConstraintType = "PrimaryKey"
	LengthConstraintType     = "Length"
	ExclusionConstraintType  = "Exclusion"
	TriggerConstraintType    = "TriggerConstraint"

	ConstraintObject = "Constraint"
)

type TransformerValidationError struct {
	ConstraintType   string
	ConstraintName   string
	ConstraintSchema string
	ConstraintDef    string
	Severity         string
	Err              error
}

func (tve *TransformerValidationError) Error() string {
	return fmt.Sprintf("%s %s", tve.Severity, tve.Err)
}

type TransformerBaseParams struct {
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
	UseType  string  `mapstructure:"useType"`
}

type TransformerBase struct {
	TransformerBaseParams
	Table         *pgDomains.TableMeta
	Column        *pgDomains.ColumnMeta
	PgType        *pgtype.Type
	EncodePlan    pgtype.EncodePlan
	TypeMap       *pgtype.Map
	SupportedOids []int
	Settings      *TransformerSettings
}

func NewTransformerBase(
	table *pgDomains.TableMeta,
	column *pgDomains.ColumnMeta,
	settings *TransformerSettings,
	params map[string]interface{},
	typeMap *pgtype.Map,
	cast interface{},
) (*TransformerBase, error) {

	if column == nil {
		return nil, fmt.Errorf("column is nil")
	}

	tParams := TransformerBaseParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if typeMap == nil {
		return nil, fmt.Errorf("typeMap cannot be nil")
	}
	var oid = column.TypeOid
	if tParams.UseType != "" {
		t, ok := typeMap.TypeForName(tParams.UseType)
		if !ok {
			return nil, fmt.Errorf("cannot find type %s", tParams.UseType)
		}
		oid = pgDomains.Oid(t.OID)
	}
	if len(settings.SupportedOids) != 0 && !slices.Contains(settings.SupportedOids, int(oid)) {
		return nil, fmt.Errorf("cannot use type: %s type is not supported", tParams.UseType)
	}
	var t *pgtype.Type
	var plan pgtype.EncodePlan
	var err error
	if cast != nil {
		t, plan, err = GetPgTypeAndEncodingPlan(typeMap, oid, cast)
		if err != nil {
			return nil, err
		}
	}

	return &TransformerBase{
		Column:                column,
		PgType:                t,
		EncodePlan:            plan,
		TypeMap:               typeMap,
		TransformerBaseParams: tParams,
		Table:                 table,
		Settings:              settings,
	}, nil
}

func (tb *TransformerBase) Validate() []error {
	var errs []error
	if tb.Nullable && tb.Column.NotNull {
		errs = append(errs, &TransformerValidationError{
			ConstraintType: NotNullConstraintType,
			Severity:       WarningErrorSeverity,
			Err:            errors.New("column cannot be null"),
		})
	}

	if tb.Settings.Variadic && tb.Column.Length != -1 {
		errs = append(errs, &TransformerValidationError{
			ConstraintType: LengthConstraintType,
			Severity:       WarningErrorSeverity,
			Err:            fmt.Errorf("possible constraint violation: column may be out of max size"),
		})
	}

	if len(tb.Table.Constraints) != 0 {
		for _, item := range tb.Table.Constraints {

			switch item.Type {
			case 'f':
				if slices.Contains(item.ReferencesColumns, tb.Column.Num) {
					errs = append(errs, &TransformerValidationError{
						ConstraintType:   FkConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: column involved into foreign key"),
					})
				}
			case 'c':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &TransformerValidationError{
						ConstraintType:   CheckConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation"),
					})
				}
			case 'p':
				if !tb.Settings.Unique {
					errs = append(errs, &TransformerValidationError{
						ConstraintType:   PkConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: transformer cannot guarantee uniqueness"),
					})
				}
				if len(item.ReferencedTable) != 0 {
					errs = append(errs, &TransformerValidationError{
						ConstraintType:   PkConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: primary key has referenced tables"),
					})
				}
			case 'u':
				if !tb.Settings.Unique {
					errs = append(errs, &TransformerValidationError{
						ConstraintType:   UniqueConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: transformer cannot guarantee uniqueness"),
					})
				}
			case 't':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &TransformerValidationError{
						ConstraintType:   TriggerConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation"),
					})
				}
			case 'x':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &TransformerValidationError{
						ConstraintType:   ExclusionConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation"),
					})
				}
			}

		}
	}

	return errs
}

func (tb *TransformerBase) Scan(src string, dest interface{}) error {
	val, err := tb.PgType.Codec.DecodeValue(tb.TypeMap, uint32(tb.Column.TypeOid), pgx.TextFormatCode, []byte(src))
	if err != nil {
		return fmt.Errorf("cannot decode value: %w", err)
	}

	return scan(val, dest)
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
