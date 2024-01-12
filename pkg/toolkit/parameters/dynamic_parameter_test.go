package parameters

import (
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

func TestDynamicParameter_Init_column_parameter(t *testing.T) {

	columnDef := toolkit.MustNewParameter("column", "some desc").
		SetIsColumn(
			toolkit.NewColumnProperties().
				SetAllowedColumnTypes("text"),
		)

	driver, _ := getDriverAndRecord(
		map[string]*toolkit.RawValue{
			"id":   toolkit.NewRawValue([]byte("123"), false),
			"data": toolkit.NewRawValue([]byte("some text"), false),
		},
	)

	p := NewDynamicParameter(columnDef, driver)
	warns, err := p.Init(nil, &toolkit.DynamicParamValue{
		Column: "data",
	})
	require.NoError(t, err)
	require.Len(t, warns, 1)
	warn := warns[0]
	require.Equal(t, toolkit.ErrorValidationSeverity, warn.Severity)
	require.Equal(t, "column parameter cannot work in dynamic mode", warn.Msg)
}

func TestDynamicParameter_Init_linked_column_parameter_unsupported_types(t *testing.T) {

	driver, _ := getDriverAndRecord(
		map[string]*toolkit.RawValue{
			"id":        toolkit.NewRawValue([]byte("123"), false),
			"date_tstz": toolkit.NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
		},
	)

	columnDef := toolkit.MustNewParameter("column", "some desc").
		SetIsColumn(
			toolkit.NewColumnProperties().
				SetAllowedColumnTypes("int2", "int4", "int8"),
		)

	columnParam := NewStaticParameter(columnDef, driver)
	warns, err := columnParam.Init(nil, toolkit.ParamsValue("id"))
	require.NoError(t, err)
	require.Empty(t, warns)

	timestampDef := toolkit.MustNewParameter("ts_val", "some desc").SetLinkParameter("column")

	timestampParam := NewDynamicParameter(timestampDef, driver)

	warns, err = timestampParam.Init(
		[]*StaticParameter{columnParam},
		&toolkit.DynamicParamValue{
			Column: "date_tstz",
		},
	)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	warn := warns[0]
	require.Equal(t, toolkit.ErrorValidationSeverity, warn.Severity)
	require.Equal(t, "linked parameter and dynamic parameter column name has different types", warn.Msg)

}

func TestDynamicParameter_Init_linked_column_parameter_supported_types(t *testing.T) {

}

func TestDynamicParameter_Init_simple(t *testing.T) {
	driver, record := getDriverAndRecord(
		map[string]*toolkit.RawValue{
			"id":        toolkit.NewRawValue([]byte("123"), false),
			"date_tstz": toolkit.NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
		},
	)

	timestampDef := toolkit.MustNewParameter("ts_val", "some desc")

	timestampParam := NewDynamicParameter(timestampDef, driver)

	warns, err := timestampParam.Init(
		[]*StaticParameter{},
		&toolkit.DynamicParamValue{
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
