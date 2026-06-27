// Copyright 2025 Greenmask
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

package context

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	commonparameters "github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

// stubTransformer is a minimal core.Transformer for snapshot tests.
type stubTransformer struct {
	name     string
	affected map[int]string
}

func (s stubTransformer) Init(context.Context) error                     { return nil }
func (s stubTransformer) Done(context.Context) error                     { return nil }
func (s stubTransformer) Transform(context.Context, core.Recorder) error { return nil }
func (s stubTransformer) GetAffectedColumns() map[int]string             { return s.affected }
func (s stubTransformer) Describe() string                               { return s.name }

// exprCond is a CondEvaluator retaining its expression.
type exprCond struct{ expr string }

func (c exprCond) Evaluate(core.Recorder) (bool, error) { return true, nil }
func (c exprCond) Expression() string                   { return c.expr }

// initStaticParam builds and initializes a static parameter with the given raw
// value (nil ⇒ rely on the definition default).
func initStaticParam(t *testing.T, def *commonparameters.ParameterDefinition, raw core.ParamsValue) *commonparameters.StaticParameter {
	t.Helper()
	p := commonparameters.NewStaticParameter(def, nil, false)
	ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
	require.NoError(t, p.Init(ctx, nil, raw))
	require.False(t, validationcollector.FromContext(ctx).HasWarnings())
	return p
}

func TestTransformerContext_GetSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		position int
		newCtx   func(t *testing.T) *TransformerContext
		assert   func(t *testing.T, ts core.TransformationSnapshot)
	}{
		{
			name:     "resolves defaults and dynamic params",
			position: 2,
			newCtx: func(t *testing.T) *TransformerContext {
				// "min" is not supplied, so its definition default ("0") must be captured.
				minParam := initStaticParam(t,
					commonparameters.MustNewParameterDefinition("min", "lower bound").SetDefaultValue([]byte("0")), nil)
				columnParam := initStaticParam(t,
					commonparameters.MustNewParameterDefinition("column", "target column"), []byte("amount"))
				dynParam := commonparameters.NewDynamicParameter(
					commonparameters.MustNewParameterDefinition("max", "upper bound"), nil)
				dynParam.DynamicValue = &core.DynamicParamValue{Column: "limit_col", CastTo: "int", Template: "{{ . }}"}
				return &TransformerContext{
					Transformer:       stubTransformer{name: "Noise", affected: map[int]string{1: "amount"}},
					Condition:         exprCond{expr: "amount > 0"},
					StaticParameters:  map[string]*commonparameters.StaticParameter{"min": minParam, "column": columnParam},
					DynamicParameters: map[string]*commonparameters.DynamicParameter{"max": dynParam},
				}
			},
			assert: func(t *testing.T, ts core.TransformationSnapshot) {
				require.Equal(t, "Noise", ts.Name)
				require.Equal(t, 2, ts.Position)
				require.Equal(t, core.ObjectFieldRef{Kind: core.FieldRefKindColumn, Value: "amount"}, ts.Field)
				require.Equal(t, core.TransformationSource{Kind: core.TransformationSourceKindExplicit}, ts.Source)

				// Resolved default captured even though "min" was not supplied.
				require.Equal(t, "0", ts.StaticParameters["min"])
				require.Equal(t, "amount", ts.StaticParameters["column"])

				// Dynamic parameter captured from its initialization settings.
				dyn, ok := ts.DynamicParameters["max"].(core.DynamicParamValue)
				require.True(t, ok)
				require.Equal(t, "limit_col", dyn.Column)
				require.Equal(t, "int", dyn.CastTo)

				require.Equal(t, []string{"amount"}, ts.AffectedColumns)
				require.Equal(t, "amount > 0", ts.Condition)

				require.NotEmpty(t, ts.StaticParametersHash)
				require.NotEmpty(t, ts.DynamicParametersHash)
				require.NotEmpty(t, ts.AffectedColumnsHash)
				require.NotEmpty(t, ts.Fingerprint)
				require.Equal(t, core.StableKey("column:amount:2:Noise"), ts.Key)
			},
		},
		{
			name:     "multiple affected columns joined and sorted",
			position: 0,
			newCtx: func(_ *testing.T) *TransformerContext {
				return &TransformerContext{
					// Affected columns out of index order to verify deterministic sorting.
					Transformer: stubTransformer{name: "Multi", affected: map[int]string{2: "last", 1: "first"}},
				}
			},
			assert: func(t *testing.T, ts core.TransformationSnapshot) {
				require.Equal(t, []string{"first", "last"}, ts.AffectedColumns)
				require.Equal(t, core.ObjectFieldRef{Kind: core.FieldRefKindColumn, Value: "first,last"}, ts.Field)
				require.Equal(t, core.StableKey("column:first,last:0:Multi"), ts.Key)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := tt.newCtx(t).GetSnapshot(tt.position)
			require.NoError(t, err)
			tt.assert(t, ts)
		})
	}
}

func TestTableDumpContext_GetSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		newCtx func(t *testing.T) *TableDumpContext
		assert func(t *testing.T, snap core.ObjectSnapshot)
	}{
		{
			name: "attributes, subset, condition and transformations",
			newCtx: func(t *testing.T) *TableDumpContext {
				columnParam := initStaticParam(t,
					commonparameters.MustNewParameterDefinition("column", "target column"), []byte("email"))
				transformer := &TransformerContext{
					Transformer:      stubTransformer{name: "RandomEmail", affected: map[int]string{1: "email"}},
					StaticParameters: map[string]*commonparameters.StaticParameter{"column": columnParam},
				}
				return &TableDumpContext{
					ColumnKind: core.EntityKindMysqlColumn,
					Table: &core.Table{
						Schema:  "app",
						Name:    "users",
						Columns: []core.Column{{Idx: 0, Name: "id", Type: core.Type{Name: "int"}}, {Idx: 1, Name: "email", Type: core.Type{Name: "varchar"}}},
					},
					Query:              "SELECT * FROM users",
					Condition:          exprCond{expr: "id > 100"},
					TransformerContext: []core.TransformerContexter{transformer},
				}
			},
			assert: func(t *testing.T, snap core.ObjectSnapshot) {
				require.Equal(t, "SELECT * FROM users", snap.SubsetQuery)
				require.NotEmpty(t, snap.SubsetQueryHash)
				require.Len(t, snap.Attributes, 2)
				require.NotEmpty(t, snap.AttributesHash)
				require.NotNil(t, snap.Condition)
				require.Equal(t, "id > 100", snap.Condition.Expression)
				require.Len(t, snap.Transformations, 1)

				// Engine-specific fields are left for the caller to overlay.
				require.Empty(t, snap.Key)
				require.Empty(t, snap.Identity.Kind)

				ts, ok := snap.Transformations["column:email:0:RandomEmail"]
				require.True(t, ok)
				require.Equal(t, "RandomEmail", ts.Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap, err := tt.newCtx(t).GetSnapshot()
			require.NoError(t, err)
			tt.assert(t, snap)
		})
	}
}
