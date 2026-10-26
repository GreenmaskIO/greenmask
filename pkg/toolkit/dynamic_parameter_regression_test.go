package toolkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDynamicParameter_Init_column_does_not_exist(t *testing.T) {
	tests := []struct {
		name          string
		paramName     string
		dynamicColumn string
	}{
		{
			name:          "missing column",
			paramName:     "min",
			dynamicColumn: "unknown_column",
		},
		{
			name:          "missing column with another parameter",
			paramName:     "max",
			dynamicColumn: "missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, _ := GetDriverAndRecord(
				map[string]*RawValue{
					"id2": NewRawValue([]byte("123"), false),
				},
			)
			def := MustNewParameterDefinition(tt.paramName, "some desc").
				SetDynamicMode(NewDynamicModeProperties())

			p := NewDynamicParameter(def, driver)
			warns, err := p.Init(nil, &DynamicParamValue{Column: tt.dynamicColumn})
			require.NoError(t, err)
			require.Len(t, warns, 1)
			warn := warns[0]
			require.Equal(t, ErrorValidationSeverity, warn.Severity)
			require.Equal(t, "column does not exist", warn.Msg)
			require.Equal(t, tt.dynamicColumn, warn.Meta["ColumnName"])
			require.Equal(t, tt.paramName, warn.Meta["ParameterName"])
			require.Equal(t, "column", warn.Meta["DynamicParameterSetting"])
		})
	}
}

func TestDynamicParameter_Scan_default_value_not_aliased(t *testing.T) {
	driver, record := GetDriverAndRecord(
		map[string]*RawValue{
			"id2":  NewRawValue([]byte("123"), false),
			"data": NewRawValue(nil, true),
		},
	)
	def := MustNewParameterDefinition("gender", "some desc").
		SetDynamicMode(NewDynamicModeProperties())
	p := NewDynamicParameter(def, driver)
	warns, err := p.Init(nil, &DynamicParamValue{Column: "data", DefaultValue: ParamsValue("M")})
	require.NoError(t, err)
	require.Empty(t, warns)
	p.SetRecord(record)

	var first string
	require.NoError(t, p.Scan(&first))
	require.Equal(t, "M", first)
	// the caller reuses its variable, the cached default must not follow it
	first = "Male"

	var second string
	require.NoError(t, p.Scan(&second))
	require.Equal(t, "M", second)
}
