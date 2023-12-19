// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type SchemaValidationFunc func(ctx context.Context, table *toolkit.Table, properties *TransformerProperties, parameters map[string]*toolkit.Parameter, types []*toolkit.Type) (toolkit.ValidationWarnings, error)

func ValidateSchema(
	table *toolkit.Table, column *toolkit.Column, columnProperties *toolkit.ColumnProperties,
) toolkit.ValidationWarnings {
	var warnings toolkit.ValidationWarnings
	for _, c := range table.Constraints {
		if w := c.IsAffected(column, columnProperties); len(w) > 0 {
			warnings = append(warnings, w...)
		}
	}
	return warnings
}

func DefaultSchemaValidator(
	ctx context.Context, table *toolkit.Table, properties *TransformerProperties, parameters map[string]*toolkit.Parameter, types []*toolkit.Type,
) (toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings

	if parameters == nil {
		return nil, nil
	}

	for _, p := range parameters {
		if !p.IsColumn || p.IsColumn && !p.ColumnProperties.Affected {
			// We assume that if parameter is not a column or is a column but not affected - it should not
			// violate constraints
			continue
		}

		// Checking is transformer can produce NULL value
		if p.ColumnProperties.Nullable && p.Column.NotNull {
			warnings = append(warnings, toolkit.NewValidationWarning().
				SetMsg("transformer may produce NULL values but column has NOT NULL constraint").
				SetSeverity(toolkit.WarningValidationSeverity).
				AddMeta("ConstraintType", toolkit.NotNullConstraintType).
				AddMeta("ParameterName", p.Name).
				AddMeta("ColumnName", p.Column.Name),
			)
		}

		// Checking transformed value will not exceed the column length
		if p.ColumnProperties.MaxLength != toolkit.WithoutMaxLength &&
			p.Column.Length < p.ColumnProperties.MaxLength {
			warnings = append(warnings, toolkit.NewValidationWarning().
				SetMsg("transformer value might be out of length range: column has a length").
				SetSeverity(toolkit.WarningValidationSeverity).
				AddMeta("ConstraintType", toolkit.LengthConstraintType).
				AddMeta("ParameterName", p.Name).
				AddMeta("ColumnName", p.Column.Name).
				AddMeta("ColumnMaxLength", p.Column.Length).
				AddMeta("TransformerMaxLength", p.ColumnProperties.MaxLength),
			)
		}

		// Performing checks constraint checks with the affected column
		for _, c := range table.Constraints {
			if p.IsColumn && (p.ColumnProperties == nil || p.ColumnProperties != nil && p.ColumnProperties.Affected) {
				if warns := c.IsAffected(p.Column, p.ColumnProperties); len(warns) > 0 {
					for _, w := range warns {
						w.AddMeta("ParameterName", p.Name)
					}
					warnings = append(warnings, warns...)
				}
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
