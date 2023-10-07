package utils

import (
	"context"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type SchemaValidationFunc func(ctx context.Context, table *toolkit.Table, properties *TransformerProperties, parameters []*toolkit.Parameter, types []*toolkit.Type) (toolkit.ValidationWarnings, error)

func DefaultSchemaValidator(
	ctx context.Context, table *toolkit.Table, properties *TransformerProperties, parameters []*toolkit.Parameter, types []*toolkit.Type,
) (toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings

	for _, p := range parameters {
		if !p.IsColumn || p.IsColumn && !p.ColumnProperties.Affected {
			// We assume that if parameter is not a column or is a column but not affected - it should not
			// violate constraints
			continue
		}

		// Checking is transformer can produce NULL value
		if !p.ColumnProperties.Nullable && p.Column.NotNull {
			warnings = append(warnings, toolkit.NewValidationWarning().
				SetMsg("transformer may produce NULL values but column has NOT NULL constraint").
				SetSeverity(toolkit.WarningValidationSeverity).
				AddMeta("ConstraintType", toolkit.NotNullConstraintType).
				AddMeta("Parameter", p.Name).
				AddMeta("Column", p.Column.Name),
			)
		}

		// Checking transformed value will not exceed the column length
		if p.ColumnProperties.MaxLength != toolkit.WithoutMaxLength &&
			p.Column.Length < p.ColumnProperties.MaxLength {
			warnings = append(warnings, toolkit.NewValidationWarning().
				SetMsg("transformer value might be out of length range: column has a length").
				SetSeverity(toolkit.WarningValidationSeverity).
				AddMeta("ConstraintType", toolkit.LengthConstraintType).
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
		idx := slices.IndexFunc(types, func(t *toolkit.Type) bool {
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
