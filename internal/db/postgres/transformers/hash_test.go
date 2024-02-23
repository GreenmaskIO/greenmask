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

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestHashTransformer_Transform_all_functions(t *testing.T) {
	columnValue := toolkit.ParamsValue("data")

	tests := []struct {
		name     string
		params   map[string]toolkit.ParamsValue
		original string
		result   string
	}{
		{
			name: "md5",
			params: map[string]toolkit.ParamsValue{
				"column":   columnValue,
				"function": []byte("md5"),
			},
			original: "123",
			result:   "202cb962ac59075b964b07152d234b70",
		},
		{
			name: "sha1",
			params: map[string]toolkit.ParamsValue{
				"column":   columnValue,
				"function": []byte("sha1"),
			},
			original: "123",
			result:   "40bd001563085fc35165329ea1ff5c5ecbdbbeef",
		},
		{
			name: "sha256",
			params: map[string]toolkit.ParamsValue{
				"column":   columnValue,
				"function": []byte("sha256"),
			},
			original: "123",
			result:   "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
		},
		{
			name: "sha512",
			params: map[string]toolkit.ParamsValue{
				"column":   columnValue,
				"function": []byte("sha512"),
			},
			original: "123",
			result:   "3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(string(tt.params["column"]), tt.original)
			transformer, warnings, err := HashTransformerDefinition.Instance(
				context.Background(),
				driver, tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)
			r, err := transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			res, err := r.GetRawColumnValueByName(string(tt.params["column"]))
			require.NoError(t, err)

			require.False(t, res.IsNull)
			require.Equal(t, tt.result, string(res.Data))

		})
	}
}

func Test_validateHashFunctionsParameter(t *testing.T) {

	tests := []struct {
		name  string
		value []byte
	}{
		{
			name:  "md5",
			value: []byte("md5"),
		},
		{
			name:  "sha1",
			value: []byte("md5"),
		},
		{
			name:  "sha256",
			value: []byte("md5"),
		},
		{
			name:  "sha512",
			value: []byte("md5"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warns, err := validateHashFunctionsParameter(nil, tt.value)
			require.NoError(t, err)
			require.Empty(t, warns)
		})
	}

	t.Run("wrong value", func(t *testing.T) {
		warns, err := validateHashFunctionsParameter(nil, []byte("md8"))
		require.NoError(t, err)
		require.Len(t, warns, 1)
		warn := warns[0]
		require.Equal(t, toolkit.ErrorValidationSeverity, warn.Severity)
		require.Equal(t, "unknown hash function name", warn.Msg)
	})

}

func TestHashTransformer_Transform_length_truncation(t *testing.T) {

	params := map[string]toolkit.ParamsValue{
		"column":     toolkit.ParamsValue("data"),
		"max_length": toolkit.ParamsValue("4"),
		"function":   toolkit.ParamsValue("sha1"),
	}
	original := "123"
	expected := "40bd"
	// Check that internal buffers wipes correctly without data lost
	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformer, warnings, err := HashTransformerDefinition.Instance(
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

	require.False(t, res.IsNull)
	require.Equal(t, expected, string(res.Data))
}

func TestHashTransformer_Transform_multiple_iterations(t *testing.T) {
	columnValue := toolkit.ParamsValue("data")

	params := map[string]toolkit.ParamsValue{
		"column":   toolkit.ParamsValue("data"),
		"function": toolkit.ParamsValue("sha1"),
	}
	original := "123"
	// Check that internal buffers wipes correctly without data lost
	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformer, warnings, err := HashTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	tests := []struct {
		name     string
		original string
		expected string
	}{
		{
			name:     "run1",
			original: "123",
			expected: "40bd001563085fc35165329ea1ff5c5ecbdbbeef",
		},
		{
			name:     "run2",
			original: "456",
			expected: "51eac6b471a284d3341d8c0c63d0f1a286262a18",
		},
		{
			name:     "run3",
			original: "789",
			expected: "fc1200c7a7aa52109d762a9f005b149abef01479",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer record.Row.Encode()

			err = record.Row.Decode([]byte(tt.original))
			require.NoError(t, err)

			_, err = transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			res, err := record.GetRawColumnValueByName(string(columnValue))
			require.NoError(t, err)

			require.False(t, res.IsNull)
			require.Equal(t, tt.expected, string(res.Data))

		})
	}
}
