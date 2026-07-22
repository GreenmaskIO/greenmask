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
// https://github.com/GreenmaskIO/greenmask/issues/465: a transformer that unconditionally produces NULL
// values (e.g. SetNull, declared with ColumnProperties.AlwaysNull = true) is applied to a column with a
// NOT NULL constraint. Because the transformer's nullability and the column's NOT NULL flag are both
// known statically (no data or config dependency), this must be reported as an ErrorValidationSeverity
// finding so that `validate`/`dump` abort by default, instead of silently allowing an unrestorable dump
// to be produced.
func TestDefaultSchemaValidator_NotNullIntoNotNullColumn_IsFatalError(t *testing.T) {
	transformerDefinition := NewTransformerDefinition(
		NewTransformerProperties("test_set_null", "sets column to NULL"),
		NewTestTransformer,
		toolkit.MustNewParameterDefinition("column", "a column name").
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetAlwaysNull(true),
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
		"a transformer that always writes NULL into a NOT NULL column must be reported as an error, not a warning")

	assert.True(t, warnings.IsFatal(),
		"ValidationWarnings containing the NotNull-into-NOT-NULL finding must be considered fatal")
}

// TestDefaultSchemaValidator_ConfigDependentNullableIntoNotNullColumn_IsWarningNotError reproduces the
// counterexample found by adversarial review of the original fix for issue #465: a transformer type that
// is merely *capable* of producing NULL depending on configuration (ColumnProperties.Nullable = true, but
// NOT ColumnProperties.AlwaysNull), e.g. Replace - whose "value" parameter determines whether a given
// configured instance can ever actually emit NULL - applied to a NOT NULL column with a static, non-null
// "value"-like configuration (mirroring Replace's own documented `value: "programmer"` example in
// docs/built_in_transformers/standard_transformers/replace.md). Unlike the AlwaysNull/SetNull case, this
// is not a static certainty: this specific configured instance can never produce NULL, so it must remain a
// WarningValidationSeverity finding (non-fatal), not ErrorValidationSeverity - otherwise `validate`/`dump`
// would hard-abort, by default and with no waiver possible, on a working, documented usage pattern.
func TestDefaultSchemaValidator_ConfigDependentNullableIntoNotNullColumn_IsWarningNotError(t *testing.T) {
	transformerDefinition := NewTransformerDefinition(
		NewTransformerProperties("test_replace", "replaces column with a configured value"),
		NewTestTransformer,
		toolkit.MustNewParameterDefinition("column", "a column name").
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				// Config-dependent capability only: this transformer type CAN produce NULL in some
				// configurations, but is not guaranteed to - unlike SetNull, it does not set AlwaysNull.
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

	// This configuration mirrors a static, always-non-null "value" (e.g. Replace's documented
	// `value: "programmer"` example): the specific instance being validated could never actually emit NULL,
	// even though the transformer type is generically Nullable.
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
	assert.Equal(t, toolkit.WarningValidationSeverity, notNullWarning.Severity,
		"a transformer that is only config-dependently Nullable (not AlwaysNull) must be reported as a "+
			"warning, not an error, so documented static-value use cases like Replace are not hard-aborted")

	assert.False(t, warnings.IsFatal(),
		"ValidationWarnings containing only a config-dependent NotNull finding must not be considered fatal")
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
