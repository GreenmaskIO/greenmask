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

package parameters

//func TestInitParametersV2(t *testing.T) {
//	driver, record := GetDriverAndRecord(
//		map[string]*RawValue{
//			"id2":       NewRawValue([]byte("123"), false),
//			"date_tstz": NewRawValue([]byte("2024-01-12 00:00:00.0+00"), false),
//		},
//	)
//
//	column := MustNewParameterDefinition("column", "column").
//		SetColumnProperties(NewColumnProperties().SetAllowedColumnTypes("date", "timestamp", "timestamptz"))
//
//	minDate := MustNewParameterDefinition("min_date", "min date").
//		LinkParameter("column").
//		SetDynamicMode(
//			NewDynamicModeProperties().
//				SetCompatibleTypes("date", "timestamp", "timestamptz"),
//		)
//
//	maxDate := MustNewParameterDefinition("max_date", "max date").
//		LinkParameter("column").
//		SetDynamicMode(
//			NewDynamicModeProperties().
//				SetCompatibleTypes("date", "timestamp", "timestamptz"),
//		)
//
//	params, warns, err := InitParameters(
//		driver,
//		[]*ParameterDefinition{column, minDate, maxDate},
//		map[string]ParamsValue{"column": []byte("date_tstz"), "max_date": []byte("2024-01-14 00:00:00.0+00")},
//		map[string]*DynamicParamValue{"min_date": {Column: "date_tstz"}},
//	)
//	require.NoError(t, err)
//	require.Empty(t, warns)
//
//	// initialize dynamic params with record
//	for _, p := range params {
//		if dp, ok := p.(*DynamicParameter); ok {
//			dp.SetRecord(record)
//		}
//	}
//
//	columnValue, ok := params["column"]
//	require.True(t, ok)
//	res, err := columnValue.Value()
//	require.NoError(t, err)
//	assert.Equal(t, "date_tstz", res)
//
//	minDateValue, ok := params["min_date"]
//	require.True(t, ok)
//	pgTimestampFormat := "2006-01-02 15:04:05.999999999Z07"
//	expected, err := time.Parse(pgTimestampFormat, "2024-01-12 00:00:00.0+00")
//	require.NoError(t, err)
//	res = time.Time{}
//	err = minDateValue.Scan(&res)
//	require.NoError(t, err)
//	assert.Equal(t, expected, res)
//
//	maxDateValue, ok := params["max_date"]
//	require.True(t, ok)
//	expected, err = time.Parse(pgTimestampFormat, "2024-01-14 00:00:00.0+00")
//	require.NoError(t, err)
//	res = time.Time{}
//	err = maxDateValue.Scan(&res)
//	require.NoError(t, err)
//	assert.Equal(t, expected, res)
//}
