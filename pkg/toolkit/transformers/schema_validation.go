package transformers

import (
	"context"
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/GreenmaskIO/greenmask/internal/domains"
)

type SchemaValidationFunc func(ctx context.Context, table *Table, properties *TransformerProperties, parameters []*Parameter, types []*Type) (ValidationWarnings, error)

func DefaultSchemaValidator(
	ctx context.Context, table *Table, properties *TransformerProperties, parameters []*Parameter, types []*Type,
) (ValidationWarnings, error) {
	var warnings ValidationWarnings

	for _, p := range parameters {
		if !p.IsColumn || p.IsColumn && !p.ColumnProperties.Affected {
			// We assume that if parameter is not a column or is a column but not affected - it should not
			// violate constraints
			continue
		}

		// Checking is transformer can produce NULL value
		if !p.ColumnProperties.Nullable && p.Column.NotNull {
			warnings = append(warnings, NewValidationWarning().
				SetMsg("transformer may produce NULL values but column has NOT NULL constraint").
				SetLevel(domains.WarningValidationSeverity).
				AddMeta("ConstraintType", NotNullConstraintType).
				AddMeta("Parameter", p.Name).
				AddMeta("Column", p.Column.Name),
			)
		}

		// Checking transformed value will not exceed the column length
		if p.ColumnProperties.MaxLength != WithoutMaxLength &&
			p.Column.Length < p.ColumnProperties.MaxLength {
			warnings = append(warnings, NewValidationWarning().
				SetMsg("transformer value might be out of length range: column has a length").
				SetLevel(domains.WarningValidationSeverity).
				AddMeta("ConstraintType", LengthConstraintType).
				AddMeta("Parameter", p.Name).
				AddMeta("Column", p.Column.Name).
				AddMeta("ColumnMaxLength", p.Column.Length).
				AddMeta("TransformerMaxLength", p.ColumnProperties.MaxLength),
			)
		}

		// Performing checks constraint checks with the affected column
		for _, c := range table.Constraints {
			if w := c.IsAffected(p); len(w) > 0 {
				warnings = append(warnings, w...)
			}
		}

		// Performing type validation
		idx := slices.IndexFunc(types, func(t *Type) bool {
			return t.Oid == p.Column.TypeOid
		})
		if idx != -1 {
			columnType := types[idx]
			w := columnType.IsAffected(p)
			warnings = append(warnings, w...)
		}

	}

	return warnings, nil
}

func DetermineConstraintType(v Constraint) string {
	switch v.(type) {
	case *Check:
		return CheckConstraintType
	case *Exclusion:
		return ExclusionConstraintType
	case *ForeignKey:
		return FkConstraintType
	case *PrimaryKey:
		return PkConstraintType
	case *Unique:
		return UniqueConstraintType
	case *TriggerConstraint:
		return TriggerConstraintType
	default:
		panic(fmt.Sprintf("unknown constraint type %v", v))
	}
}
