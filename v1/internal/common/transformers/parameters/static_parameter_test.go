package parameters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func TestStaticParameter_Init(t *testing.T) {
	t.Run("column success", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(&commonmodels.Column{
				Name:     "test_column",
				TypeName: "int2",
			}, true)

		parameter := NewStaticParameter(columnDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, []byte("test_column"))
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
	})

	t.Run("column unknown", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(nil, false)

		parameter := NewStaticParameter(columnDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, []byte("test_column"))
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.Contains(t, vc.GetWarnings()[0].Msg, "column does not exist")
	})

	t.Run("column type is not allowed", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(&commonmodels.Column{
				Name:     "test_column",
				TypeName: "text",
			}, true)

		parameter := NewStaticParameter(columnDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, []byte("test_column"))
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.Contains(t, vc.GetWarnings()[0].Msg, "unsupported column type")
	})

	t.Run("string value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetAllowedValues([]byte("valid value2"), []byte("valid value"))
		parameter := NewStaticParameter(columnDef, nil)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, []byte("valid value"))
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
	})

	t.Run("invalid value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetAllowedValues([]byte("valid value2"), []byte("valid value"))
		parameter := NewStaticParameter(columnDef, nil)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, []byte("invalid value"))
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.Contains(t, vc.GetWarnings()[0].Msg, "unknown parameter value")
	})

	t.Run("raw value validator error", func(t *testing.T) {
		validator := func(
			vc *validationcollector.Collector,
			p *ParameterDefinition,
			v commonmodels.ParamsValue,
		) error {
			vc.Add(commonmodels.NewValidationWarning().
				SetSeverity(commonmodels.ValidationSeverityError).
				SetMsg("Test warning"))
			return commonmodels.ErrFatalValidationError
		}
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetRawValueValidator(validator)
		parameter := NewStaticParameter(columnDef, nil)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, []byte("any"))
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.Contains(t, vc.GetWarnings()[0].Msg, "Test warning")
	})

	t.Run("raw value validator success", func(t *testing.T) {
		validator := func(
			vc *validationcollector.Collector,
			p *ParameterDefinition,
			v commonmodels.ParamsValue,
		) error {
			return nil
		}
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetRawValueValidator(validator)
		parameter := NewStaticParameter(columnDef, nil)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, []byte("any"))
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
	})

	t.Run("required parameter is empty", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetRequired(true)
		parameter := NewStaticParameter(columnDef, nil)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, nil)
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.Contains(t, vc.GetWarnings()[0].Msg, "parameter is required")
	})

	t.Run("required parameter is empty but has a default value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetDefaultValue([]byte("default value")).
			SetRequired(true)
		parameter := NewStaticParameter(columnDef, nil)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, nil)
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
		require.Equal(t, parameter.rawValue, commonmodels.ParamsValue("default value"))
	})

	t.Run("not required parameter is empty but has a default value", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("param", "some desc").
			SetDefaultValue([]byte("default value"))
		parameter := NewStaticParameter(columnDef, nil)
		vc := validationcollector.NewCollector()
		err := parameter.Init(vc, nil, nil)
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
		require.Equal(t, parameter.rawValue, commonmodels.ParamsValue("default value"))
	})

	t.Run("link column parameter", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		tableDriver := newTableDriverMock()
		tableDriver.On("GetColumnByName", "test_column").
			Return(&commonmodels.Column{
				Name:     "test_column",
				TypeName: "int2",
			}, true)

		columnParameter := NewStaticParameter(columnDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := columnParameter.Init(vc, nil, []byte("test_column"))
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())

		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			LinkParameter("column")
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver)
		vc = validationcollector.NewCollector()
		err = linkedParameter.Init(
			vc,
			map[string]*StaticParameter{
				"column": columnParameter,
			},
			[]byte("test_column"),
		)
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
		assert.Equal(t, linkedParameter.linkedColumnParameter, columnParameter)
	})

	t.Run("unknown linked column parameter", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			LinkParameter("column")
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := linkedParameter.Init(vc, nil, []byte("test_column"))
		require.ErrorIs(t, err, errParameterIsNotFound)
	})

	t.Run("template support and tmpl provided", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := linkedParameter.Init(vc, nil, []byte("{{ 1 }}"))
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
		assert.Equal(t, linkedParameter.rawValue, commonmodels.ParamsValue("1"))
	})

	t.Run("template support and just a raw value", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := linkedParameter.Init(vc, nil, []byte("1"))
		require.NoError(t, err)
		assert.False(t, vc.HasWarnings())
		assert.Equal(t, linkedParameter.rawValue, commonmodels.ParamsValue("1"))
	})

	t.Run("template parsing error", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := linkedParameter.Init(vc, nil, []byte("{{ asad }}"))
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.HasWarnings())
		assert.Equal(t, vc.GetWarnings()[0].Msg, "error parsing template in the parameter")
	})

	t.Run("template execution error", func(t *testing.T) {
		linkedParameterDef := MustNewParameterDefinition("min", "min val").
			SetSupportTemplate(true)
		tableDriver := newTableDriverMock()
		linkedParameter := NewStaticParameter(linkedParameterDef, tableDriver)
		vc := validationcollector.NewCollector()
		err := linkedParameter.Init(vc, nil, []byte(`{{ "asdad" | noiseInt 0.2 }}`))
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.HasWarnings())
		assert.Equal(t, vc.GetWarnings()[0].Msg, "error executing template in the parameter")
	})
}
