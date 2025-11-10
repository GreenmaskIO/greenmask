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

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

type SchemaValidationFunc func(
	ctx context.Context,
	table commonmodels.Table,
	properties *TransformerProperties,
	parameters map[string]*commonparameters.StaticParameter,
) error

func isConstraintAffected(constraintColumns []string, transformingColumn string) bool {
	return slices.Contains(constraintColumns, transformingColumn)
}

func DefaultSchemaValidator(
	ctx context.Context,
	table commonmodels.Table,
	properties *TransformerProperties,
	parameters map[string]*commonparameters.StaticParameter,
) error {
	if parameters == nil {
		return nil
	}

	for _, p := range parameters {
		if !p.GetDefinition().IsColumn || p.GetDefinition().IsColumn && !p.GetDefinition().ColumnProperties.Affected {
			// We assume that if parameter is not a column or is a column but not affected - it should not
			// violate constraints
			continue
		}
		ctx = validationcollector.WithMeta(ctx,
			commonmodels.MetaKeyParameterName, p.GetDefinition().Name,
			commonmodels.MetaKeyColumnName, p.Column.Name,
		)

		// Checking is transformer can produce NULL value
		if p.GetDefinition().ColumnProperties.Nullable && p.Column.NotNull {
			validationcollector.FromContext(ctx).
				Add(commonmodels.NewValidationWarning().
					SetMsg("transformer may produce NULL values but column has NOT NULL constraint").
					SetSeverity(commonmodels.ValidationSeverityWarning).
					AddMeta("ConstraintType", toolkit.NotNullConstraintType).
					AddMeta("ParameterName", p.GetDefinition().Name).
					AddMeta("ColumnName", p.Column.Name))
		}

		// Checking transformed value will not exceed the column length
		if p.GetDefinition().ColumnProperties.MaxLength != toolkit.WithoutMaxLength &&
			p.Column.Length < p.GetDefinition().ColumnProperties.MaxLength {
			validationcollector.FromContext(ctx).
				Add(commonmodels.NewValidationWarning().
					SetMsg("transformer value might be out of length range: column has a length").
					SetSeverity(commonmodels.ValidationSeverityWarning).
					AddMeta("ConstraintType", toolkit.LengthConstraintType).
					AddMeta("ParameterName", p.GetDefinition().Name).
					AddMeta("ColumnName", p.Column.Name).
					AddMeta("ColumnMaxLength", p.Column.Length).
					AddMeta("TransformerMaxLength", p.GetDefinition().ColumnProperties.MaxLength))
		}

		// Performing checks constraint checks with the affected column
		for _, c := range table.Constraints {
			if isConstraintAffected(c.Columns(), p.Column.Name) {
				validationcollector.FromContext(ctx).
					Add(commonmodels.NewValidationWarning().
						SetSeverity(commonmodels.ValidationSeverityWarning).
						AddMeta("ConstraintName", c.Name()).
						AddMeta("ConstraintType", c.Type()).
						AddMeta("ConstraintColumns", c.Columns()).
						SetMsg("potential constraint violation detected"))
			}
		}
		// TODO: add type constraint violation checks.
		// TODO: check transformer properties.
	}

	return nil
}
