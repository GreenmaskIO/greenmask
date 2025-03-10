package parameters

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
			"id2":  NewRawValue([]byte("123"), false),
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
	require.Equal(t, "parameter does not support dynamic mode", warn.Msg)
}

func TestDynamicParameter_Init_linked_column_parameter_unsupported_types(t *testing.T) {

	driver, _ := GetDriverAndRecord(
		map[string]*RawValue{
			"id2":       NewRawValue([]byte("123"), false),
			"date_tstz": NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
		},
	)

	columnDef := MustNewParameterDefinition("column", "some desc").
		SetIsColumn(
			NewColumnProperties().
				SetAllowedColumnTypes("int2", "int4", "int8"),
		)

	columnParam := NewStaticParameter(columnDef, driver)
	warns, err := columnParam.Init(nil, ParamsValue("id2"))
	require.NoError(t, err)
	require.Empty(t, warns)

	timestampDef := MustNewParameterDefinition("ts_val", "some desc").
		SetLinkParameter("column").
		SetDynamicMode(
			NewDynamicModeProperties().
				SetCompatibleTypes("int2"),
		)

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
	driver, _ := GetDriverAndRecord(
		map[string]*RawValue{
			"id2":       NewRawValue([]byte("123"), false),
			"date_tstz": NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
			"date_date": NewRawValue([]byte("2024-01-12"), false),
		},
	)

	columnDef := MustNewParameterDefinition("column", "some desc").
		SetIsColumn(
			NewColumnProperties().
				SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
		)

	columnParam := NewStaticParameter(columnDef, driver)
	warns, err := columnParam.Init(nil, ParamsValue("date_date"))
	require.NoError(t, err)
	require.Empty(t, warns)

	timestampDef := MustNewParameterDefinition("ts_val", "some desc").
		SetLinkParameter("column").
		SetDynamicMode(
			NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		)

	timestampParam := NewDynamicParameter(timestampDef, driver)

	warns, err = timestampParam.Init(
		map[string]*StaticParameter{columnDef.Name: columnParam},
		&DynamicParamValue{
			Column: "date_tstz",
		},
	)
	require.NoError(t, err)
	require.Empty(t, warns)

}

func TestDynamicParameter_Value_simple(t *testing.T) {
	driver, record := GetDriverAndRecord(
		map[string]*RawValue{
			"id2":       NewRawValue([]byte("123"), false),
			"date_tstz": NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
		},
	)

	timestampDef := MustNewParameterDefinition("ts_val", "some desc").
		SetDynamicMode(
			NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		)

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
