package toolkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDynamicParameter_Init_column_parameter(t *testing.T) {

	columnDef := MustNewParameterDefinition("column", "some desc").
		SetIsColumn(
			NewColumnProperties().
				SetAllowedColumnTypes("text"),
		)

	driver, _ := GetDriverAndRecord(
		map[string]*RawValue{
			"id":   NewRawValue([]byte("123"), false),
			"data": NewRawValue([]byte("some text"), false),
		},
	)

	p := NewDynamicParameter(columnDef, driver)
	warns, err := p.Init(nil, &DynamicParamValue{
		Column: "data",
	})
	require.NoError(t, err)
	require.Len(t, warns, 1)
	warn := warns[0]
	require.Equal(t, ErrorValidationSeverity, warn.Severity)
	require.Equal(t, "column parameter cannot work in dynamic mode", warn.Msg)
}

func TestDynamicParameter_Init_linked_column_parameter_unsupported_types(t *testing.T) {

	driver, _ := GetDriverAndRecord(
		map[string]*RawValue{
			"id":        NewRawValue([]byte("123"), false),
			"date_tstz": NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
		},
	)

	columnDef := MustNewParameterDefinition("column", "some desc").
		SetIsColumn(
			NewColumnProperties().
				SetAllowedColumnTypes("int2", "int4", "int8"),
		)

	columnParam := NewStaticParameter(columnDef, driver)
	warns, err := columnParam.Init(nil, ParamsValue("id"))
	require.NoError(t, err)
	require.Empty(t, warns)

	timestampDef := MustNewParameterDefinition("ts_val", "some desc").SetLinkParameter("column")

	timestampParam := NewDynamicParameter(timestampDef, driver)

	warns, err = timestampParam.Init(
		map[string]*StaticParameter{columnDef.Name: columnParam},
		&DynamicParamValue{
			Column: "date_tstz",
		},
	)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	warn := warns[0]
	require.Equal(t, ErrorValidationSeverity, warn.Severity)
	require.Equal(t, "linked parameter and dynamic parameter column name has different types", warn.Msg)

}

func TestDynamicParameter_Init_linked_column_parameter_supported_types(t *testing.T) {

}

func TestDynamicParameter_Value_simple(t *testing.T) {
	driver, record := GetDriverAndRecord(
		map[string]*RawValue{
			"id":        NewRawValue([]byte("123"), false),
			"date_tstz": NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
		},
	)

	timestampDef := MustNewParameterDefinition("ts_val", "some desc")

	timestampParam := NewDynamicParameter(timestampDef, driver)

	warns, err := timestampParam.Init(
		nil,
		&DynamicParamValue{
			Column: "date_tstz",
		},
	)
	require.NoError(t, err)
	require.Empty(t, warns)

	timestampParam.SetRecord(record)

	value, err := timestampParam.Value()
	require.NoError(t, err)
	require.NotEmpty(t, value)
}
