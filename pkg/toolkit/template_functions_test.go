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
	"bytes"
	"strconv"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuncMap_getNullValue(t *testing.T) {
	expected := NullValue
	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- null -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)
	err = tmpl.Execute(buf, nil)
	require.NoError(t, err)
	res := buf.String()
	require.Equal(t, string(expected), res)
}

func TestFuncMap_isNull(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- . | isNull -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	tests := []struct {
		name     string
		params   any
		expected string
	}{
		{
			name:     "exists",
			params:   NullValue,
			expected: "true",
		},
		{

			name:     "doe not exists",
			params:   1,
			expected: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			require.NoError(t, err)
			res := buf.String()
			require.Equal(t, tt.expected, res)
			buf.Reset()
		})
	}

}

func TestFuncMap_nullCoalesce(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- sqlCoalesce .A .B .C -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		A any
		B any
		C any
	}

	tests := []struct {
		name     string
		params   params
		expected string
	}{
		{
			name: "not null value 1",
			params: params{
				A: NullValue,
				B: 1,
				C: 2,
			},
			expected: "1",
		},
		{
			name: "not null value 2",
			params: params{
				A: NullValue,
				B: NullValue,
				C: 2,
			},
			expected: "2",
		},
		{
			name: "null value",
			params: params{
				A: NullValue,
				B: NullValue,
				C: NullValue,
			},
			expected: string(NullValue),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			require.NoError(t, err)
			res := buf.String()
			require.Equal(t, tt.expected, res)
			buf.Reset()
		})
	}

}

func TestFuncMap_jsonExists(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Json | jsonExists .Path -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Json string
		Path string
	}

	tests := []struct {
		name     string
		params   params
		expected string
	}{
		{
			name: "exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a",
			},
			expected: "true",
		},
		{

			name: "does not exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a.b",
			},
			expected: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			require.NoError(t, err)
			res := buf.String()
			require.Equal(t, tt.expected, res)
			buf.Reset()
		})
	}

}

func TestFuncMap_mustJsonGet(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Json | mustJsonGet .Path -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Json string
		Path string
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    string
	}{
		{
			name: "exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a",
			},
			expected: "1",
		},
		{

			name: "does not exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a.b",
			},
			expectedErr: "json path \"data.a.b\" does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				require.Equal(t, tt.expected, res)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_mustJsonGetRaw(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Json | mustJsonGetRaw .Path | isString -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Json string
		Path string
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    string
	}{
		{
			name: "exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a",
			},
			expected: "true",
		},
		{

			name: "does not exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a.b",
			},
			expectedErr: "json path \"data.a.b\" does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				require.Equal(t, tt.expected, res)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_jsonGet(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Json | jsonGet .Path | isNil -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Json string
		Path string
	}

	tests := []struct {
		name     string
		params   params
		expected string
	}{
		{
			name: "exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a",
			},
			expected: "false",
		},
		{

			name: "does not exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a.b",
			},
			expected: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			require.NoError(t, err)
			res := buf.String()
			require.Equal(t, tt.expected, res)
			buf.Reset()
		})
	}
}

func TestFuncMap_jsonGetRaw(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Json | jsonGetRaw .Path | eq "" -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Json string
		Path string
	}

	tests := []struct {
		name     string
		params   params
		expected string
	}{
		{
			name: "exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a",
			},
			expected: "false",
		},
		{

			name: "does not exists",
			params: params{
				Json: `{"data": {"a": 1}}`,
				Path: "data.a.b",
			},
			expected: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			require.NoError(t, err)
			res := buf.String()
			require.Equal(t, tt.expected, res)
			buf.Reset()
		})
	}
}

func TestFuncMap_masking(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Data | masking .Type -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Data string
		Type string
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    string
	}{
		{
			name: "mobile type",
			params: params{
				Data: `+35787535472`,
				Type: "mobile",
			},
			expected: "+357***35472",
		},
		{
			name: "default type",
			params: params{
				Data: `+35787535472`,
				Type: "default",
			},
			expected: "************",
		},
		{

			name: "wrong type",
			params: params{
				Data: `+35787535472`,
				Type: "wrong_type",
			},
			expectedErr: "wrong type masking \"wrong_type\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				require.Equal(t, tt.expected, res)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_truncateDate(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Date | truncateDate .Part -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Date time.Time
		Part string
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    time.Time
	}{
		{
			name: "ok",
			params: params{
				Date: time.Date(2023, 11, 23, 1, 2, 3, 4, time.Now().Location()),
				Part: "month",
			},
			expected: time.Date(2023, 11, 1, 0, 0, 0, 0, time.Now().Location()),
		},
		{

			name: "wrong_type",
			params: params{
				Date: time.Date(2023, 11, 23, 1, 2, 3, 4, time.Now().Location()),
				Part: "wrong_type",
			},
			expectedErr: "wrong part value \"wrong_type\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				require.Equal(t, tt.expected.String(), res)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_noiseDate(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Date | noiseDatePgInterval .Interval -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Date     time.Time
		Interval string
	}

	type expected struct {
		minDate time.Time
		maxDate time.Time
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    expected
	}{
		{
			name: "ok",
			params: params{
				Date:     time.Date(2023, 11, 23, 1, 10, 3, 4, time.Now().Location()),
				Interval: "1 year 1 mon 00:03:00",
			},
			expected: expected{
				minDate: time.Date(2022, 10, 23, 1, 8, 3, 4, time.Now().Location()),
				maxDate: time.Date(2024, 11, 23, 1, 12, 3, 4, time.Now().Location()),
			},
		},
		{

			name: "invalid syntax",
			params: params{
				Date:     time.Date(2023, 11, 23, 1, 2, 3, 4, time.Now().Location()),
				Interval: "wq1 msha",
			},
			expectedErr: "error parsing \"interval\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				resDate, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", res)
				require.NoError(t, err)
				assert.WithinRangef(t, resDate, tt.expected.minDate, tt.expected.maxDate, "result is out of range")
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_noiseFloat(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Value | noiseFloat .Ratio 4 -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Value float64
		Ratio float64
	}

	type expected struct {
		min float64
		max float64
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    expected
	}{
		{
			name: "ok",
			params: params{
				Value: 2.8,
				Ratio: 0.1,
			},
			expected: expected{
				min: 2.8 - 2.8*0.1,
				max: 2.8 + 2.8*0.1,
			},
		},
		{

			name: "invalid syntax",
			params: params{
				Value: 2.8,
				Ratio: -0.1,
			},
			expectedErr: "ratio must be in interval (0, 1] got -0.100000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				resFloat, err := strconv.ParseFloat(res, 64)
				require.NoError(t, err)
				assert.Truef(t, resFloat >= tt.expected.min && resFloat <= tt.expected.max, "result is out of range")
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_noiseInt(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- .Value | noiseInt .Ratio -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Value int64
		Ratio float64
	}

	type expected struct {
		min int64
		max int64
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    expected
	}{
		{
			name: "ok",
			params: params{
				Value: 100,
				Ratio: 0.1,
			},
			expected: expected{
				min: 90,
				max: 110,
			},
		},
		{

			name: "invalid syntax",
			params: params{
				Value: 100,
				Ratio: -0.1,
			},
			expectedErr: "ratio must be in interval (0, 1] got -0.100000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				resFloat, err := strconv.ParseInt(res, 10, 64)
				require.NoError(t, err)
				assert.Truef(t, resFloat >= tt.expected.min && resFloat <= tt.expected.max, "result is out of range")
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_randomBool(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- randomBool -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)
	err = tmpl.Execute(buf, nil)
	require.NoError(t, err)
	res := buf.String()
	require.NoError(t, err)
	assert.Truef(t, res == "true" || res == "false", "result is out of range")
}

func TestFuncMap_randomDate(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- randomDate .Min .Max -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Min time.Time
		Max time.Time
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
	}{
		{
			name: "mobile type",
			params: params{
				Min: time.Date(2022, 10, 23, 1, 8, 3, 4, time.Now().Location()),
				Max: time.Date(2024, 11, 23, 1, 12, 3, 4, time.Now().Location()),
			},
		},
		{

			name: "wrong range",
			params: params{
				Max: time.Date(2022, 10, 23, 1, 8, 3, 4, time.Now().Location()),
				Min: time.Date(2024, 11, 23, 1, 12, 3, 4, time.Now().Location()),
			},
			expectedErr: "must before the max date",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				resDate, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", res)
				require.NoError(t, err)
				require.NoError(t, err)
				assert.WithinRangef(t, resDate, tt.params.Min, tt.params.Max, "result is out of range")
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_randomFloat(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- randomFloat .Min .Max .Precision -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Min       float64
		Max       float64
		Precision int
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
	}{
		{
			name: "ok",
			params: params{
				Min:       0.1,
				Max:       9.1,
				Precision: 4,
			},
		},
		{
			name: "wrong precision",
			params: params{
				Min:       0,
				Max:       9.1,
				Precision: -4,
			},
			expectedErr: " precision must be 0 or higher got -4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				resFloat, err := strconv.ParseFloat(res, 64)
				require.NoError(t, err)
				assert.Truef(t, resFloat >= tt.params.Min && resFloat <= tt.params.Max, "result is out of range")
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_randomInt(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- randomInt .Min .Max -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Min int64
		Max int64
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
	}{
		{
			name: "ok",
			params: params{
				Min: -100,
				Max: 100,
			},
		},
		{
			name: "wrong min value",
			params: params{
				Min: 100,
				Max: -100,
			},
			expectedErr: " min value (100) must be less than the max (-100)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				resFloat, err := strconv.ParseInt(res, 10, 64)
				require.NoError(t, err)
				assert.Truef(t, resFloat >= tt.params.Min && resFloat <= tt.params.Max, "result is out of range")
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_randomString(t *testing.T) {

	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- randomString .Min .Max -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)

	type params struct {
		Min int64
		Max int64
	}

	tests := []struct {
		name        string
		params      params
		expectedErr string
		expected    string
	}{
		{
			name: "ok",
			params: params{
				Min: 10,
				Max: 20,
			},
			expected: "[abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890]{10,20}",
		},
		{
			name: "wrong min value",
			params: params{
				Min: 100,
				Max: -100,
			},
			expectedErr: "maxLengthInt must be higher or equal 0 got -100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tmpl.Execute(buf, tt.params)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				res := buf.String()
				require.NoError(t, err)
				assert.Regexp(t, tt.expected, res)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}

			buf.Reset()
		})
	}
}

func TestFuncMap_roundFloat(t *testing.T) {
	expected := "1.2346"
	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- 1.23456789 | roundFloat 4 -}}
	`
	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)
	err = tmpl.Execute(buf, nil)
	require.NoError(t, err)
	res := buf.String()
	require.Equal(t, expected, res)
}

func TestFuncMap_timeToUnix(t *testing.T) {
	expected := "1706373033"
	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- . | timeToUnix "sec" -}}
	`
	obj := time.Unix(1706373033, 0)

	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)
	err = tmpl.Execute(buf, obj)
	require.NoError(t, err)
	res := buf.String()
	require.Equal(t, expected, res)
}

func TestFuncMap_unixToTime(t *testing.T) {
	//expected :=
	fmt := "2006-01-02 15:04:05.999999999 -0700 MST"
	expected, err := time.Parse(fmt, "2024-01-27 18:30:33 +0200 EET")
	require.NoError(t, err)
	buf := bytes.NewBuffer(nil)
	tmplStr := `
		{{- . | unixToTime "sec" -}}
	`
	obj := 1706373033

	tmpl, err := template.New("test").Funcs(FuncMap()).Parse(tmplStr)
	require.NoError(t, err)
	err = tmpl.Execute(buf, obj)
	require.NoError(t, err)
	res, err := time.Parse(fmt, buf.String())
	require.NoError(t, err)
	require.Equal(t, expected, res)
}
