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

package parameters

import (
	"context"
	"testing"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticParameter_Init(t *testing.T) {
	t.Run("column success", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(&core.Column{
				Name: "test_column",
				Type: core.Type{
					Name: "int2",
				},
			}, nil)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		parameter := NewStaticParameter(columnDef, tableDriver, false)
		err := parameter.Init(ctx, nil, []byte("test_column"))
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
	})

	t.Run("column unknown", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(nil, core.ErrUnknownColumnName)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		parameter := NewStaticParameter(columnDef, tableDriver, false)
		err := parameter.Init(ctx, nil, []byte("test_column"))
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).IsFatal())
		assert.Contains(t, validationcollector.FromContext(ctx).GetWarnings()[0].Err.Error(),
			"unknown column name")
	})

	t.Run("column type is not allowed", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(&core.Column{
				Name: "test_column",
				Type: core.Type{
					Name: "text",
				},
			}, nil)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		parameter := NewStaticParameter(columnDef, tableDriver, false)
		err := parameter.Init(ctx, nil, []byte("test_column"))
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).IsFatal())
		assert.Contains(t,
			validationcollector.FromContext(ctx).GetWarnings()[0].Msg,
			"column type is not allowed",
		)
	})

	t.Run("string value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetAllowedValues([]byte("valid value2"), []byte("valid value"))
		parameter := NewStaticParameter(columnDef, nil, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := parameter.Init(ctx, nil, []byte("valid value"))
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
	})

	t.Run("invalid value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetAllowedValues([]byte("valid value2"), []byte("valid value"))
		parameter := NewStaticParameter(columnDef, nil, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := parameter.Init(ctx, nil, []byte("invalid value"))
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).IsFatal())
		assert.Contains(t, validationcollector.FromContext(ctx).GetWarnings()[0].Msg,
			"unknown parameter value")
	})

	t.Run("raw value validator error", func(t *testing.T) {
		validator := func(
			ctx context.Context,
			p *ParameterDefinition,
			v core.ParamsValue,
		) error {
			validationcollector.FromContext(ctx).
				Add(core.NewValidationWarning().
					SetSeverity(core.ValidationSeverityError).
					SetMsg("Test warning"))
			return core.ErrFatalValidationError
		}
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetRawValueValidator(validator)
		parameter := NewStaticParameter(columnDef, nil, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := parameter.Init(ctx, nil, []byte("any"))
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).IsFatal())
		assert.Contains(t, validationcollector.FromContext(ctx).GetWarnings()[0].Msg, "Test warning")
	})

	t.Run("raw value validator success", func(t *testing.T) {
		validator := func(
			ctx context.Context,
			p *ParameterDefinition,
			v core.ParamsValue,
		) error {
			return nil
		}
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetRawValueValidator(validator)
		parameter := NewStaticParameter(columnDef, nil, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := parameter.Init(ctx, nil, []byte("any"))
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
	})

	t.Run("required parameter is empty", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetRequired(true)
		parameter := NewStaticParameter(columnDef, nil, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := parameter.Init(ctx, nil, nil)
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).IsFatal())
		assert.Contains(t, validationcollector.FromContext(ctx).GetWarnings()[0].Msg, "parameter is required")
	})

	t.Run("required parameter is empty but has a default value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetDefaultValue([]byte("default value")).
			SetRequired(true)
		parameter := NewStaticParameter(columnDef, nil, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := parameter.Init(ctx, nil, nil)
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		require.Equal(t, parameter.rawValue, core.ParamsValue("default value"))
	})

	t.Run("not required parameter is empty but has a default value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetDefaultValue([]byte("default value"))
		parameter := NewStaticParameter(columnDef, nil, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := parameter.Init(ctx, nil, nil)
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		require.Equal(t, parameter.rawValue, core.ParamsValue("default value"))
	})

	t.Run("link column parameter", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(&core.Column{
				Name: "test_column",
				Type: core.Type{
					Name: "int2",
				},
			}, nil)

		columnParameter := NewStaticParameter(columnDef, tableDriver, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := columnParameter.Init(ctx, nil, []byte("test_column"))
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())

		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			LinkParameter("column")
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver, false)
		err = linkedParameter.Init(
			ctx,
			map[string]*StaticParameter{
				"column": columnParameter,
			},
			[]byte("test_column"),
		)
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		assert.Equal(t, linkedParameter.linkedColumnParameter, columnParameter)
	})

	t.Run("unknown linked column parameter", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			LinkParameter("column")
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := linkedParameter.Init(ctx, nil, []byte("test_column"))
		require.ErrorIs(t, err, errParameterIsNotFound)
	})

	t.Run("template support and tmpl provided", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := linkedParameter.Init(ctx, nil, []byte("{{ 1 }}"))
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		assert.Equal(t, linkedParameter.rawValue, core.ParamsValue("1"))
	})

	t.Run("template support and just a raw value", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := linkedParameter.Init(ctx, nil, []byte("1"))
		require.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		assert.Equal(t, linkedParameter.rawValue, core.ParamsValue("1"))
	})

	t.Run("template parsing error", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := linkedParameter.Init(ctx, nil, []byte("{{ asad }}"))
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).HasWarnings())
		assert.Equal(t,
			validationcollector.FromContext(ctx).GetWarnings()[0].Msg,
			"error parsing template in the parameter")
	})

	t.Run("template execution error", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver, false)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
		err := linkedParameter.Init(ctx, nil, []byte(`{{ "asdad" | noiseInt 0.2 }}`))
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).HasWarnings())
		assert.Equal(t, validationcollector.FromContext(ctx).GetWarnings()[0].Msg,
			"error executing template in the parameter")
	})
}
