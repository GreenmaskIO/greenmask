package transformers

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/slices"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
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

func (tb *TransformerBase) Validate() domains.TransformerValidationErrors {
	var errs []error
	if tb.Nullable && tb.Column.NotNull {
		errs = append(errs, &domains.TransformerValidationError{
			ConstraintType: domains.NotNullConstraintType,
			Severity:       domains.WarningErrorSeverity,
			Err:            errors.New("column cannot be null"),
		})
	}

	if tb.Settings.Variadic && tb.Column.Length != -1 {
		errs = append(errs, &domains.TransformerValidationError{
			ConstraintType: domains.LengthConstraintType,
			Severity:       domains.WarningErrorSeverity,
			Err:            fmt.Errorf("possible constraint violation: column may be out of max size"),
		})
	}

	if len(tb.Table.Constraints) != 0 {
		for _, item := range tb.Table.Constraints {

			switch item.Type {
			case 'f':
				if slices.Contains(item.ReferencesColumns, tb.Column.Num) {
					errs = append(errs, &domains.TransformerValidationError{
						ConstraintType:   domains.FkConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         domains.WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: column involved into foreign key"),
					})
				}
			case 'c':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &domains.TransformerValidationError{
						ConstraintType:   domains.CheckConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         domains.WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation"),
					})
				}
			case 'p':
				if !tb.Settings.Unique && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &domains.TransformerValidationError{
						ConstraintType:   domains.PkConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         domains.WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: transformer cannot guarantee uniqueness"),
					})
				}
				if len(item.ReferencedTable) != 0 && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &domains.TransformerValidationError{
						ConstraintType:   domains.PkConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         domains.WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: primary key has referenced tables"),
					})
				}
			case 'u':
				if !tb.Settings.Unique && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &domains.TransformerValidationError{
						ConstraintType:   domains.UniqueConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         domains.WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation: transformer cannot guarantee uniqueness"),
					})
				}
			case 't':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &domains.TransformerValidationError{
						ConstraintType:   domains.TriggerConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         domains.WarningErrorSeverity,
						Err:              fmt.Errorf("possible constraint violation"),
					})
				}
			case 'x':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					errs = append(errs, &domains.TransformerValidationError{
						ConstraintType:   domains.ExclusionConstraintType,
						ConstraintName:   item.Name,
						ConstraintSchema: item.Schema,
						ConstraintDef:    item.Def,
						Severity:         domains.WarningErrorSeverity,
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
