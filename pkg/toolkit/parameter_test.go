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

package toolkit

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDriver() *Driver {
	table := &Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*Column{
			{
				Name:     "id",
				TypeName: "int2",
				TypeOid:  pgtype.Int2OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "created_at",
				TypeName: "timestamp",
				TypeOid:  pgtype.TimestampOID,
				Num:      2,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "title",
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
				Num:      3,
				NotNull:  true,
				Length:   -1,
			},
		},
		Constraints: []Constraint{},
	}
	driver, _, err := NewDriver(table, nil)
	if err != nil {
		panic(err.Error())
	}
	return driver
}

func TestParameter_Parse_simple(t *testing.T) {

	driver := getDriver()

	p1 := MustNewParameterDefinition(
		"simple_param",
		"Simple description",
	)

	warnings, err := p1.Init(driver, nil, []*ParameterDefinition{p1}, []byte("1"))
	require.NoError(t, err)
	assert.Empty(t, warnings)
	var expected = 1
	var res int
	_, err = p1.Scan(&res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestParameter_Parse_with_allowed_pg_types(t *testing.T) {

	driver := getDriver()

	// Check simple column parameter definition positive case
	p1 := MustNewParameterDefinition(
		"column",
		"Simple column parameter",
	).SetRequired(true).
		SetIsColumn(&ColumnProperties{
			Nullable:     false,
			Affected:     true,
			AllowedTypes: []string{"date", "timestamp", "timestamptz"},
		})

	//warnings, err := p1.Decode(driver, rawParams, nil, nil)
	warnings, err := p1.Init(driver, nil, []*ParameterDefinition{p1}, []byte("created_at"))
	require.NoError(t, err)
	assert.Empty(t, warnings)
	var expected = "created_at"
	res, err := p1.Value()
	assert.NoError(t, err)
	assert.Equal(t, expected, res)

	// Check simple column parameter definition negative case
	warnings, err = p1.Init(driver, nil, []*ParameterDefinition{p1}, []byte("id"))
	require.NoError(t, err)
	assert.NotEmpty(t, warnings)
	assert.True(t, slices.ContainsFunc(warnings, func(warning *ValidationWarning) bool {
		return warning.Msg == "unsupported column type"
	}))
}

func TestParameter_Parse_with_linked_parameter(t *testing.T) {

	driver := getDriver()

	// Check simple linked parameter definition positive case
	columnParam := MustNewParameterDefinition(
		"column",
		"Simple column parameter",
	).SetRequired(true).
		SetIsColumn(NewColumnProperties())

	linkedParam := MustNewParameterDefinition(
		"replace",
		"Simple column parameter",
	).SetRequired(true).
		SetLinkParameter("column")

	params := []*ParameterDefinition{columnParam, linkedParam}

	warnings, err := columnParam.Init(driver, nil, params, []byte("created_at"))
	require.NoError(t, err)
	assert.Empty(t, warnings)

	warnings, err = linkedParam.Init(driver, nil, params, []byte("2023-08-27 00:00:00.000000"))
	require.NoError(t, err)
	assert.Empty(t, warnings)

	res := time.Time{}
	expected := time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC)
	_, err = linkedParam.Scan(&res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestParameter_scan_empty(t *testing.T) {

	driver := getDriver()

	p1 := MustNewParameterDefinition(
		"simple_param",
		"Simple description",
	)

	warnings, err := p1.Init(driver, nil, []*ParameterDefinition{p1}, nil)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	var res int
	isEmpty, err := p1.Scan(&res)
	require.NoError(t, err)
	assert.True(t, isEmpty)
}

func TestParameter_structured_value_validation(t *testing.T) {

	type TestType struct {
		A string `json:"a"`
	}

	driver := getDriver()
	res := &TestType{}
	expected := &TestType{
		A: "test",
	}

	p1 := MustNewParameterDefinition(
		"simple_param",
		"Simple description",
	).SetRawValueValidator(func(p *ParameterDefinition, v ParamsValue) (ValidationWarnings, error) {
		err := json.Unmarshal(v, res)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})

	warnings, err := p1.Init(driver, nil, []*ParameterDefinition{p1}, []byte(`{"a": "test"}`))
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Equal(t, expected.A, res.A)
}

func TestInitParametersV2(t *testing.T) {
	driver, record := GetDriverAndRecord(
		map[string]*RawValue{
			"id":        NewRawValue([]byte("123"), false),
			"date_tstz": NewRawValue([]byte("2024-01-12 00:00:00.0+00"), false),
		},
	)

	column := MustNewParameterDefinition("column", "column").
		SetIsColumn(NewColumnProperties().SetAllowedColumnTypes("date", "timestamp", "timestamptz"))

	minDate := MustNewParameterDefinition("min_date", "min date").
		SetLinkParameter("column")

	maxDate := MustNewParameterDefinition("max_date", "max date").
		SetLinkParameter("column")

	params, warns, err := InitParametersV2(
		driver,
		[]*ParameterDefinition{column, minDate, maxDate},
		map[string]ParamsValue{"column": []byte("date_tstz"), "max_date": []byte("2024-01-14 00:00:00.0+00")},
		map[string]*DynamicParamValue{"min_date": {Column: "date_tstz"}},
	)
	require.NoError(t, err)
	require.Empty(t, warns)

	// initialize dynamic params with record
	for _, p := range params {
		if dp, ok := p.(*DynamicParameter); ok {
			dp.SetRecord(record)
		}
	}

	columnValue, empty := params["column"]
	require.True(t, empty)
	res, err := columnValue.Value()
	require.NoError(t, err)
	assert.Equal(t, "date_tstz", res)

	minDateValue, empty := params["min_date"]
	require.True(t, empty)
	pgTimestampFormat := "2006-01-02 15:04:05.999999999Z07"
	expected, err := time.Parse(pgTimestampFormat, "2024-01-12 00:00:00.0+00")
	require.NoError(t, err)
	res = time.Time{}
	empty, err = minDateValue.Scan(&res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)

	maxDateValue, empty := params["max_date"]
	require.True(t, empty)
	expected, err = time.Parse(pgTimestampFormat, "2024-01-14 00:00:00.0+00")
	require.NoError(t, err)
	res = time.Time{}
	empty, err = maxDateValue.Scan(&res)
	require.NoError(t, err)
	assert.False(t, empty)
	assert.Equal(t, expected, res)
}
