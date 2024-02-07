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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestJsonTransformer_Transform(t *testing.T) {
	var attrName = "doc"
	var originalValue = `{"name":{"last":"Anderson", "age": 5, "todelete": true}}`
	var expectedValue = toolkit.NewValue(`{"name":{"last":"Test","first":"Sara", "age": 10}}`, false)
	driver, record := getDriverAndRecord(attrName, originalValue)
	transformer, warnings, err := JsonTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column": toolkit.ParamsValue(attrName),
			"operations": toolkit.ParamsValue(`[
				{"operation": "set", "path": "name.first", "value": "Sara"},
				{"operation": "set", "path": "name.last", "value": "Test"},
				{"operation": "set", "path": "name.age", "value": 10},
				{"operation": "delete", "path": "name.todelete"}
			]`),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.GetRawColumnValueByName(attrName)
	require.NoError(t, err)

	require.Equal(t, expectedValue.IsNull, res.IsNull)
	expected := expectedValue.Value.(string)
	resValue := string(res.Data)
	require.JSONEq(t, expected, resValue)
}

func TestJsonTransformer_Transform_with_template(t *testing.T) {
	ops := []*Operation{
		{
			Operation:     "set",
			Path:          "name.ts",
			ValueTemplate: "{{- .GetOriginalValue | .DecodeValueByType \"timestamptz\" | noiseDatePgInterval \"1 year 6 mon 1 day\" | .EncodeValueByType \"timestamptz\" | toJsonRawValue -}}",
		},
	}
	minValue := time.UnixMilli(1653249289277332000 / int64(time.Millisecond))
	maxValue := time.UnixMilli(1748116489277332000 / int64(time.Millisecond))

	opsData, err := json.Marshal(ops)
	require.NoError(t, err)

	var attrName = "doc"
	var originalValue = `{"name":{"last":"Anderson", "age": 5, "todelete": true, "ts": "2023-11-23 19:54:49.277332+00"}}`
	driver, record := getDriverAndRecord(attrName, originalValue)

	transformer, warnings, err := JsonTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column":     toolkit.ParamsValue(attrName),
			"operations": opsData,
		},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.GetRawColumnValueByName(attrName)
	require.NoError(t, err)

	require.False(t, res.IsNull)
	resStr := gjson.GetBytes(res.Data, "name.ts").Str
	resAny, err := driver.DecodeValueByTypeName("timestamptz", []byte(resStr))
	require.NoError(t, err)
	resTime := resAny.(time.Time)
	assert.WithinRange(t, resTime, minValue, maxValue)
}

func TestJsonTransformer_Transform_null(t *testing.T) {
	var expectedValue = toolkit.NewValue(`{"name":{"test":"test"}}`, false)
	ops := []*Operation{
		{
			Operation:     "set",
			Path:          "name.test",
			ValueTemplate: "\"test\"",
		},
	}

	opsData, err := json.Marshal(ops)
	require.NoError(t, err)

	var attrName = "doc"
	var originalValue = `\N`
	driver, record := getDriverAndRecord(attrName, originalValue)

	transformer, warnings, err := JsonTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column":     toolkit.ParamsValue(attrName),
			"operations": opsData,
			"keep_null":  toolkit.ParamsValue("false"),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.GetRawColumnValueByName(attrName)
	require.NoError(t, err)
	require.Equal(t, expectedValue.IsNull, res.IsNull)
	expected := expectedValue.Value.(string)
	resValue := string(res.Data)
	require.JSONEq(t, expected, resValue)
}

func TestJsonTransformer_structure_tags_encoding_regression(t *testing.T) {
	// The unexpected behavior described in issue https://github.com/GreenmaskIO/greenmask/issues/4
	// The problem was that the Operation object did not have an appropriate json tag on the fields
	rawData := []byte(`
		[
			{
				"operation": "set", 
				"path": "name", 
				"value_template": "template_tes", 
				"value": "value_test", 
				"error_not_exist": true
			}
		]
	`)

	expected := &Operation{
		Operation:     "set",
		Path:          "name",
		ValueTemplate: "template_tes",
		Value:         "value_test",
		ErrorNotExist: true,
	}

	var ops []*Operation
	err := json.Unmarshal(rawData, &ops)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	op := ops[0]
	assert.Equal(t, expected.Operation, op.Operation)
	assert.Equal(t, expected.Path, op.Path)
	assert.Equal(t, expected.ValueTemplate, op.ValueTemplate)
	assert.Equal(t, expected.Value, op.Value)
	assert.Equal(t, expected.ErrorNotExist, op.ErrorNotExist)
}
