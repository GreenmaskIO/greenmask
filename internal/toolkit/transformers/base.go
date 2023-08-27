package transformers

import (
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/slices"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/data_section"
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
	Column   string  `mapstructure:"column"`
}

type TransformerBase struct {
	TransformerBaseParams
	Table         *data_section.Table
	Column        *data_section.Column
	PgType        *pgtype.Type
	EncodePlan    pgtype.EncodePlan
	TypeMap       *pgtype.Map
	SupportedOids []int
	Settings      *TransformerSettings
	Params        map[string]interface{}
	ColumnNum     int
}

// NewTransformerBase - initialise and check the transformer requirements depending on transformer type and it settings
func NewTransformerBase(
	table *data_section.Table,
	settings *TransformerSettings,
	params map[string]interface{},
	typeMap *pgtype.Map,
	cast interface{},
) (*TransformerBase, error) {
	var err error

	tParams := TransformerBaseParams{
		Fraction: DefaultNullFraction,
	}
	if err = ParseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if settings.Name == "" {
		return nil, fmt.Errorf("fix transformer implementation: transformer name was not assigned")
	}

	var columnNum int
	var t *pgtype.Type
	var plan pgtype.EncodePlan
	var column *data_section.Column
	if settings.TransformationType == domains.AttributeTransformation {
		if tParams.Column == "" {
			return nil, fmt.Errorf("column parameter must be set")
		}
		if typeMap == nil {
			return nil, fmt.Errorf("typeMap cannot be nil")
		}
		columnNum = slices.IndexFunc(table.Columns, func(column *data_section.Column) bool {
			return column.Name == tParams.Column
		})
		if columnNum == -1 {
			return nil, fmt.Errorf(`column "%s" not found`, tParams.Column)
		}
		column = table.Columns[columnNum]
		oid := column.TypeOid
		if tParams.UseType != "" {
			t, ok := typeMap.TypeForName(tParams.UseType)
			if !ok {
				return nil, fmt.Errorf("cannot find type %s", tParams.UseType)
			}
			oid = data_section.Oid(t.OID)
		}
		if len(settings.SupportedOids) != 0 && !slices.Contains(settings.SupportedOids, int(oid)) {
			return nil, fmt.Errorf("cannot use type: %s type is not supported", tParams.UseType)
		}

		if cast != nil {
			t, plan, err = GetPgTypeAndEncodingPlan(typeMap, oid, cast)
			if err != nil {
				return nil, err
			}
		}
	}

	return &TransformerBase{
		PgType:                t,
		EncodePlan:            plan,
		TypeMap:               typeMap,
		TransformerBaseParams: tParams,
		Table:                 table,
		Settings:              settings,
		Params:                params,
		ColumnNum:             columnNum,
		Column:                column,
	}, nil
}

func (tb *TransformerBase) IsCustom() bool {
	return tb.Settings.IsCustom
}

func (tb *TransformerBase) GetTransformationType() domains.TransformationType {
	return tb.Settings.TransformationType
}

func (tb *TransformerBase) GetParam(name string) (interface{}, bool) {
	val, ok := tb.Params[name]
	return val, ok
}

func (tb *TransformerBase) GetName() string {
	return tb.Settings.Name
}

func (tb *TransformerBase) Validate() (domains.ValidationWarnings, error) {
	// There must be logic according to the
	var warnings domains.ValidationWarnings
	if tb.Nullable && tb.Column.NotNull {
		warnings = append(warnings, domains.NewValidationWarning().
			SetMsg("column cannot be null").
			SetLevel(domains.ErrorValidationSeverity).
			AddMeta("ConstraintType", NotNullConstraintType),
		)
	}

	if tb.Settings.Variadic && tb.Column.Length != -1 {
		warnings = append(warnings, domains.NewValidationWarning().
			SetMsg("possible constraint violation: column may be out of max size").
			SetLevel(domains.WarningValidationSeverity).
			AddMeta("ConstraintType", LengthConstraintType),
		)
	}

	if len(tb.Table.Constraints) != 0 {
		for _, item := range tb.Table.Constraints {

			switch item.ConstraintType {
			case 'f':
				if slices.Contains(item.ReferencesColumns, tb.Column.Num) {
					warnings = append(warnings, domains.NewValidationWarning().
						SetMsg("possible constraint violation: column is involved into foreign key").
						SetLevel(domains.WarningValidationSeverity).
						AddMeta("ConstraintType", FkConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Definition),
					)
				}
			case 'c':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					warnings = append(warnings, domains.NewValidationWarning().
						SetMsg("possible constraint violation").
						SetLevel(domains.WarningValidationSeverity).
						AddMeta("ConstraintType", CheckConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Definition),
					)
				}
			case 'p':
				if !tb.Settings.Unique && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					warnings = append(warnings, domains.NewValidationWarning().
						SetMsg("possible constraint violation: transformer cannot guarantee uniqueness").
						SetLevel(domains.WarningValidationSeverity).
						AddMeta("ConstraintType", PkConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Definition),
					)
				}
				if len(item.ReferencedTables) != 0 && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					warnings = append(warnings, domains.NewValidationWarning().
						SetMsg("possible constraint violation: primary key has referenced tables").
						SetLevel(domains.WarningValidationSeverity).
						AddMeta("ConstraintType", ReferencesConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Definition),
					)
				}
			case 'u':
				if !tb.Settings.Unique && slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					warnings = append(warnings, domains.NewValidationWarning().
						SetMsg("possible constraint violation: transformer cannot guarantee uniqueness").
						SetLevel(domains.WarningValidationSeverity).
						AddMeta("ConstraintType", UniqueConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Definition),
					)
				}
			case 't':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					warnings = append(warnings, domains.NewValidationWarning().
						SetMsg("possible constraint violation").
						SetLevel(domains.WarningValidationSeverity).
						AddMeta("ConstraintType", TriggerConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Definition),
					)
				}
			case 'x':
				if slices.Contains(item.ConstrainedColumns, tb.Column.Num) {
					warnings = append(warnings, domains.NewValidationWarning().
						SetMsg("possible constraint violation").
						SetLevel(domains.WarningValidationSeverity).
						AddMeta("ConstraintType", ExclusionConstraintType).
						AddMeta("ConstraintName", item.Name).
						AddMeta("ConstraintSchema", item.Schema).
						AddMeta("ConstraintDef", item.Definition),
					)
				}
			}

		}
	}

	return warnings, nil
}

func (tb *TransformerBase) Scan(src string, dest interface{}) error {
	val, err := tb.PgType.Codec.DecodeValue(tb.TypeMap, uint32(tb.Column.TypeOid), pgx.TextFormatCode, []byte(src))
	if err != nil {
		return fmt.Errorf("cannot scan value: %w", err)
	}

	return scan(val, dest)
}

//func ParseTransformerParams(src map[string]interface{}, dest interface{}) error {
//	if err := mapstructure.Decode(src, dest); err != nil {
//		return fmt.Errorf("parameters parsing error: %w", err)
//	}
//
//	if err := validate.Struct(dest); err != nil {
//		var errs validator.ValidationErrors
//		switch {
//		case errors.As(err, &errs):
//			var firstErr string
//			for _, item := range errs.Translate(translators) {
//				if firstErr == "" {
//					firstErr = item
//				}
//				log.Warn().Msg(item)
//			}
//			return fmt.Errorf("validation error: %s", firstErr)
//		default:
//			return fmt.Errorf("validation error: %w", err)
//		}
//	}
//	return nil
//}
