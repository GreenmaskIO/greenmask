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
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// TestDefaultSchemaValidator_NotNullIntoNotNullColumn_IsFatalError reproduces the scenario from
// https://github.com/GreenmaskIO/greenmask/issues/465: a transformer that may produce NULL values
// (e.g. SetNull) is applied to a column with a NOT NULL constraint. Because the transformer's
// nullability and the column's NOT NULL flag are both known statically (no data dependency),
// this must be reported as an ErrorValidationSeverity finding so that `validate`/`dump` abort by
// default, instead of silently allowing an unrestorable dump to be produced.
func TestDefaultSchemaValidator_NotNullIntoNotNullColumn_IsFatalError(t *testing.T) {
	transformerDefinition := NewTransformerDefinition(
		NewTransformerProperties("test_set_null", "sets column to NULL"),
		NewTestTransformer,
		toolkit.MustNewParameterDefinition("column", "a column name").
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetNullable(true),
			),
	)

	table := &toolkit.Table{
		Schema: "public",
		Name:   "demo",
		Oid:    1224,
		Columns: []*toolkit.Column{
			{
				Name:     "id",
				TypeName: "int8",
				TypeOid:  pgtype.Int8OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "code",
				TypeName: "varchar",
				TypeOid:  pgtype.VarcharOID,
				Num:      2,
				NotNull:  true,
				Length:   255,
			},
		},
		Constraints: []toolkit.Constraint{},
	}

	driver, _, err := toolkit.NewDriver(table, nil)
	require.NoError(t, err)

	rawParams := map[string]toolkit.ParamsValue{
		"column": []byte("code"),
	}

	_, warnings, err := transformerDefinition.Instance(context.Background(), driver, rawParams, nil, "", false)
	require.NoError(t, err)
	require.NotEmpty(t, warnings)

	idx := -1
	for i, w := range warnings {
		if ct, ok := w.Meta["ConstraintType"]; ok && ct == toolkit.NotNullConstraintType {
			idx = i
			break
		}
	}
	require.NotEqual(t, -1, idx, "expected a NotNull constraint warning to be produced")

	notNullWarning := warnings[idx]
	assert.Equal(t, toolkit.ErrorValidationSeverity, notNullWarning.Severity,
		"a transformer that may write NULL into a NOT NULL column must be reported as an error, not a warning")

	assert.True(t, warnings.IsFatal(),
		"ValidationWarnings containing the NotNull-into-NOT-NULL finding must be considered fatal")
}

// TestDefaultSchemaValidator_DataDependentConstraints_RemainWarnings ensures the severity change for the
// NotNull check does not widen to the other, data-dependent constraint checks (Unique, Check, etc.),
// which can only ever indicate a *possible* violation since greenmask cannot know the actual row values
// at validate time. Those must remain WarningValidationSeverity and non-fatal.
func TestDefaultSchemaValidator_DataDependentConstraints_RemainWarnings(t *testing.T) {
	transformerDefinition := NewTransformerDefinition(
		NewTransformerProperties("test_random", "writes a non-null value"),
		NewTestTransformer,
		toolkit.MustNewParameterDefinition("column", "a column name").
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetNullable(false),
			),
	)

	table := &toolkit.Table{
		Schema: "public",
		Name:   "demo",
		Oid:    1224,
		Columns: []*toolkit.Column{
			{
				Name:     "id",
				TypeName: "int8",
				TypeOid:  pgtype.Int8OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "email",
				TypeName: "varchar",
				TypeOid:  pgtype.VarcharOID,
				Num:      2,
				// Nullable column: the NotNull check must not fire here, isolating the assertions
				// below to the data-dependent constraint checks.
				NotNull: false,
				Length:  255,
			},
		},
	}
	table.Constraints = []toolkit.Constraint{
		toolkit.NewUnique("public", "uq_demo_email", "UNIQUE (email)", 2000, []toolkit.AttNum{table.Columns[1].Num}),
		toolkit.NewCheck("public", "ck_demo_email", "CHECK (email <> '')", 2001, []toolkit.AttNum{table.Columns[1].Num}),
	}

	driver, _, err := toolkit.NewDriver(table, nil)
	require.NoError(t, err)

	rawParams := map[string]toolkit.ParamsValue{
		"column": []byte("email"),
	}

	_, warnings, err := transformerDefinition.Instance(context.Background(), driver, rawParams, nil, "", false)
	require.NoError(t, err)
	require.NotEmpty(t, warnings)

	seenTypes := map[string]bool{}
	for _, w := range warnings {
		ct, ok := w.Meta["ConstraintType"]
		if !ok {
			continue
		}
		ctStr, _ := ct.(string)
		seenTypes[ctStr] = true

		require.NotEqual(t, toolkit.NotNullConstraintType, ctStr,
			"NotNull warning should not fire for a nullable column in this test")

		assert.Equal(t, toolkit.WarningValidationSeverity, w.Severity,
			"data-dependent constraint %q must remain WarningValidationSeverity", ctStr)
	}

	assert.True(t, seenTypes[toolkit.UniqueConstraintType], "expected a Unique constraint warning")
	assert.True(t, seenTypes[toolkit.CheckConstraintType], "expected a Check constraint warning")

	assert.False(t, warnings.IsFatal(),
		"data-dependent constraint warnings (Unique/Check) must not be treated as fatal")
}
