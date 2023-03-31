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

// TODO: Template transformer. Functionality
// TemplateContext:
//	* GetRawValue - toolkit.RawValue
//	* GetValue - toolkit.Value
//	* GetTValue - special value wrappers for templates that implements OPs with types
//
//
// Provide functions:
//  Obfuscation functions:
//  	* NoiseInt
//  	* NoiseFloat
//  	* NoiseDate
//  	* NoiseTimestamp
//  	* NoiseTimestampTz
//		* IntervalNoise
//  	* RandomInt
//  	* RandomFloat
//  	* RandomDate
//  	* RandomTimestamp
//  	* RandomTimestampTz
//  	* JsonSet
//  	* JsonDelete
//  	* JsonGet
//  Cast function:
//   	* ToInt
//   	* ToFloat
//   	* ToString
//   	* ToInterval
//      * ToJson
//	Array ops:
//		* ArrayFind
//		* ArraySlice
//		* ArrayAppend
//		* ArrayPrepend
//		* ArrayConcatenate
//

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestTemplateTransformer_Transform_int(t *testing.T) {
	var columnName = "id"
	var template = `
		{{- $val := .GetValue -}}
		{{- if isNull $val -}}
			{{- null -}}
		{{- else if eq $val 1 }}
			{{- 123 -}}
		{{- else -}}
			{{- add $val 10 | .EncodeValue -}}
		{{- end -}}
	`
	var originalValue = "3"
	var expectedValue = "13"

	driver, record := getDriverAndRecord(columnName, originalValue)

	transformer, warnings, err := TemplateTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column":   toolkit.ParamsValue(columnName),
			"template": toolkit.ParamsValue(template),
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
	encoded, err := r.Encode()
	require.NoError(t, err)
	res, err := encoded.Encode()
	require.NoError(t, err)
	assert.Equal(t, expectedValue, string(res))
}

func TestTemplateTransformer_Transform_timestamp(t *testing.T) {
	var columnName = "date_ts"
	var template = `
		{{- $val := .GetValue -}}
		{{- if isNull $val -}}
			{{- now | .EncodeValue -}}
		{{- else -}}
			{{- $val | .EncodeValue -}}			
		{{- end -}}
	`
	var originalValue = "\\N"
	var expectedPattern = `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`

	driver, record := getDriverAndRecord(columnName, originalValue)

	transformer, warnings, err := TemplateTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column":   toolkit.ParamsValue(columnName),
			"template": toolkit.ParamsValue(template),
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
	encoded, err := r.Encode()
	require.NoError(t, err)
	res, err := encoded.Encode()
	require.NoError(t, err)
	require.Regexp(t, expectedPattern, string(res))
}
