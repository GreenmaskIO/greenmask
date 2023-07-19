package transformers

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"golang.org/x/exp/slices"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	FkConstraintType         = "ForeignKey"
	CheckConstraintType      = "Check"
	NotNullConstraintType    = "Check"
	PkConstraintType         = "PrimaryKey"
	UniqueConstraintType     = "Unique"
	ReferencesConstraintType = "PrimaryKey"
	LengthConstraintType     = "Length"
	ExclusionConstraintType  = "Exclusion"
	TriggerConstraintType    = "TriggerConstraint"
)

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

func (tb *TransformerBase) IsCustom() bool {
	return tb.Settings.IsCustom
}

func (tb *TransformerBase) Validate() domains.RuntimeErrors {
	var errs domains.RuntimeErrors
	if tb.Nullable && tb.Column.NotNull {
		errs = append(errs, domains.NewRuntimeError().
			SetErr(errors.New("column cannot be null")).
			SetLevel(zerolog.WarnLevel).
			AddMeta("ConstraintType", NotNullConstraintType),
		)
	}

	if tb.Settings.Variadic && tb.Column.Length != -1 {
		errs = append(errs, domains.NewRuntimeError().
			SetErr(fmt.Errorf("possible constraint violation: column may be out of max size")).
			SetLevel(zerolog.WarnLevel).
			AddMeta("ConstraintType", LengthConstraintType),
		)
	}

	if len(tb.Table.Constraints) != 0 {
		for _, item := range tb.Table.Constraints {

			switch item.Type {
			case 'f':
				if slices.Contains(item.ReferencesColumns, tb.Column.Num) {
					errs = append(errs, domains.NewRuntimeError().
						SetErr(fmt.Errorf("possible constraint violation: column involved into foreign key")).
						SetLevel(zerolog.WarnLevel).
						AddMeta("ConstraintType", FkConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Def),
					)
				}
			case 'c':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, domains.NewRuntimeError().
						SetErr(fmt.Errorf("possible constraint violation")).
						SetLevel(zerolog.WarnLevel).
						AddMeta("ConstraintType", CheckConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Def),
					)
				}
			case 'p':
				if !tb.Settings.Unique && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, domains.NewRuntimeError().
						SetErr(fmt.Errorf("possible constraint violation: transformer cannot guarantee uniqueness")).
						SetLevel(zerolog.WarnLevel).
						AddMeta("ConstraintType", PkConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Def),
					)
				}
				if len(item.ReferencedTable) != 0 && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, domains.NewRuntimeError().
						SetErr(fmt.Errorf("possible constraint violation: primary key has referenced tables")).
						SetLevel(zerolog.WarnLevel).
						AddMeta("ConstraintType", ReferencesConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Def),
					)
				}
			case 'u':
				if !tb.Settings.Unique && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, domains.NewRuntimeError().
						SetErr(fmt.Errorf("possible constraint violation: transformer cannot guarantee uniqueness")).
						SetLevel(zerolog.WarnLevel).
						AddMeta("ConstraintType", UniqueConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Def),
					)
				}
			case 't':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, domains.NewRuntimeError().
						SetErr(fmt.Errorf("possible constraint violation")).
						SetLevel(zerolog.WarnLevel).
						AddMeta("ConstraintType", TriggerConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Def),
					)
				}
			case 'x':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, domains.NewRuntimeError().
						SetErr(fmt.Errorf("possible constraint violation")).
						SetLevel(zerolog.WarnLevel).
						AddMeta("ConstraintType", ExclusionConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Def),
					)
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
