package transformers

import (
	"fmt"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	FkConstraintType           = "ForeignKey"
	CheckConstraintType        = "Check"
	NotNullConstraintType      = "NotNull"
	PkConstraintType           = "PrimaryKey"
	PkConstraintReferencesType = "PrimaryKeyReferences"
	UniqueConstraintType       = "Unique"
	LengthConstraintType       = "Length"
	ExclusionConstraintType    = "Exclusion"
	TriggerConstraintType      = "TriggerConstraint"
)

type SchemaValidationFunc func(table *Table, properties *Properties, parameters []*Parameter, types *[]Type) (ValidationWarnings, error)

func DefaultSchemaValidator(
	table *Table, properties *Properties, parameters []*Parameter, types *[]Type,
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
				SetMsg("transformer may generate NULL values but column has NOT NULL constraint").
				SetLevel(domains.WarningValidationSeverity).
				AddMeta("ConstraintType", NotNullConstraintType).
				AddMeta("Parameter", p.Name).
				AddMeta("Column", p.Column.Name),
			)
		}

		// Checking transformed value will not exceed the column length
		if p.ColumnProperties.MaxLength != ColumnWithoutMaxLength &&
			p.Column.Length < p.ColumnProperties.MaxLength {
			warnings = append(warnings, NewValidationWarning().
				SetMsg("column value may be out of length range").
				SetLevel(domains.WarningValidationSeverity).
				AddMeta("ConstraintType", LengthConstraintType).
				AddMeta("Parameter", p.Name).
				AddMeta("Column", p.Column.Name),
			)
		}

		// Performing checks constraint checks with the affected column
		for _, c := range table.Constraints {
			if c.IsAffected(p.Column) {
				warnings = append(warnings, NewValidationWarning().
					SetMsg("possible constraint violation").
					SetLevel(domains.WarningValidationSeverity).
					AddMeta("ConstraintType", DetermineConstraintType(c)).
					AddMeta("ConstraintMetadata", c))
			}
		}

	}

	return nil, fmt.Errorf("type validation is not implemented")

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
	case *PrimaryKeyReferences:
		return PkConstraintReferencesType
	case *Unique:
		return UniqueConstraintType
	case *TriggerConstraint:
		return TriggerConstraintType
	default:
		panic(fmt.Sprintf("unknown constraint type %v", v))
	}
}
