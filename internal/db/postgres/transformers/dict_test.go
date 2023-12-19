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

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestDictTransformer_Transform_with_fail(t *testing.T) {

	original := "2023-11-10"
	expected := "2023-01-01"

	params := map[string]toolkit.ParamsValue{
		"column":           toolkit.ParamsValue("date_date"),
		"values":           toolkit.ParamsValue(`{"2023-11-10": "2023-01-01", "2023-11-11": "2023-01-02"}`),
		"fail_not_matched": toolkit.ParamsValue(`true`),
		"validate":         toolkit.ParamsValue(`true`),
	}

	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformer, warnings, err := DictTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	res, err := r.GetRawColumnValueByName(string(params["column"]))
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	require.Equal(t, expected, string(res.Data))

	original = "2023-11-11"
	expected = "2023-01-02"
	_, record = getDriverAndRecord(string(params["column"]), original)
	r, err = transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err = r.GetRawColumnValueByName(string(params["column"]))
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	require.Equal(t, expected, string(res.Data))

}

func TestDictTransformer_Transform_validation_error(t *testing.T) {

	original := "2023-11-10"

	params := map[string]toolkit.ParamsValue{
		"column":   toolkit.ParamsValue("date_date"),
		"values":   toolkit.ParamsValue(`{"2023-11-10": "value_error"}`),
		"validate": toolkit.ParamsValue(`true`),
	}

	driver, _ := getDriverAndRecord(string(params["column"]), original)
	_, warnings, err := DictTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, warnings)

	params = map[string]toolkit.ParamsValue{
		"column":   toolkit.ParamsValue("date_date"),
		"values":   toolkit.ParamsValue(`{"2023-11-14": "2023-11-10"}`),
		"default":  toolkit.ParamsValue(`asdnakmsd`),
		"validate": toolkit.ParamsValue(`true`),
	}

	driver, _ = getDriverAndRecord(string(params["column"]), original)
	_, warnings, err = DictTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, warnings)
}

func TestDictTransformer_Transform_error_not_matched(t *testing.T) {
	original := "2022-10-10"

	params := map[string]toolkit.ParamsValue{
		"column":           toolkit.ParamsValue("date_date"),
		"values":           toolkit.ParamsValue(`{"2023-11-10": "2023-01-01"}`),
		"fail_not_matched": toolkit.ParamsValue(`true`),
		"validate":         toolkit.ParamsValue(`true`),
	}

	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformer, warnings, err := DictTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
	_, err = transformer.Transform(
		context.Background(),
		record,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, `unable to match value for`)
}

func TestDictTransformer_Transform_use_default(t *testing.T) {
	original := "2022-10-10"
	expected := "2024-11-10"

	params := map[string]toolkit.ParamsValue{
		"column":           toolkit.ParamsValue("date_date"),
		"values":           toolkit.ParamsValue(`{"2023-11-10": "2023-01-01"}`),
		"fail_not_matched": toolkit.ParamsValue(`true`),
		"validate":         toolkit.ParamsValue(`true`),
		"default":          toolkit.ParamsValue(expected),
	}

	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformer, warnings, err := DictTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.GetRawColumnValueByName(string(params["column"]))
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	require.Equal(t, expected, string(res.Data))
}

func TestDictTransformer_Transform_with_int_values(t *testing.T) {

	original := "1"
	expected := "2"

	params := map[string]toolkit.ParamsValue{
		"column":           toolkit.ParamsValue("id"),
		"values":           toolkit.ParamsValue(`{"1": "2", "3": "4"}`),
		"fail_not_matched": toolkit.ParamsValue(`true`),
		"validate":         toolkit.ParamsValue(`true`),
	}

	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformer, warnings, err := DictTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	res, err := r.GetRawColumnValueByName(string(params["column"]))
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	require.Equal(t, expected, string(res.Data))

}
