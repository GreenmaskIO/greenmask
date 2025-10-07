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

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func TestHashTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		columnName       string
		staticParameters map[string]commonmodels.ParamsValue
		dynamicParameter map[string]commonmodels.DynamicParamValue
		original         string
		expected         string
		validateFn       func(t *testing.T, originalEmail, transformedEmail string)
		expectedErr      string
		columns          []commonmodels.Column
		isNull           bool
	}{
		{
			name: "md5",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("md5"),
			},
			original:   "123",
			expected:   "202cb962ac59075b964b07152d234b70",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "sha1",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("sha1"),
			},
			original:   "123",
			expected:   "40bd001563085fc35165329ea1ff5c5ecbdbbeef",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "sha256",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("sha256"),
			},
			original:   "123",
			expected:   "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "sha512",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("sha512"),
			},
			original:   "123",
			expected:   "3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "sha3-224",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("sha3-224"),
			},
			original:   "123",
			expected:   "602bdc204140db016bee5374895e5568ce422fabe17e064061d80097",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "sha3-254",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("sha3-254"),
			},
			original:   "123",
			expected:   "a03ab19b866fc585b5cb1812a2f63ca861e7e7643ee5d43fd7106b623725fd67",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "sha3-384",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("sha3-384"),
			},
			original:   "123",
			expected:   "9bd942d1678a25d029b114306f5e1dae49fe8abeeacd03cfab0f156aa2e363c988b1c12803d4a8c9ba38fdc873e5f007",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "sha3-512",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"function": commonmodels.ParamsValue("sha3-512"),
			},
			original:   "123",
			expected:   "48c8947f69c054a5caa934674ce8881d02bb18fb59d5a63eeaddff735b0e9801e87294783281ae49fc8287a0fd86779b27d7972d3e84f0fa0d826d7cb67dfefc",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
		{
			name: "length truncation",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":     commonmodels.ParamsValue("data"),
				"function":   commonmodels.ParamsValue("sha3-512"),
				"max_length": commonmodels.ParamsValue("4"),
			},
			original:   "123",
			expected:   "48c8",
			isNull:     false,
			columnName: "data",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
			validateFn: func(t *testing.T, expected, actual string) {
				require.Equal(t, expected, actual)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				HashTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, commonmodels.NewColumnRawValue([]byte(tt.original), tt.isNull))

			err = env.Transform(t, ctx)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			rec := env.GetRecord()
			val, err := rec.GetRawColumnValueByName(tt.columnName)
			require.NoError(t, err)
			require.Equal(t, tt.isNull, val.IsNull)
			if !tt.isNull && tt.validateFn != nil {
				tt.validateFn(t, tt.expected, string(val.Data))
			}
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
			value: []byte("sha1"),
		},
		{
			name:  "sha256",
			value: []byte("sha256"),
		},
		{
			name:  "sha512",
			value: []byte("sha512"),
		},
		{
			name:  "sha3-224",
			value: []byte("sha3-224"),
		},
		{
			name:  "sha3-254",
			value: []byte("sha3-254"),
		},
		{
			name:  "sha3-384",
			value: []byte("sha3-384"),
		},
		{
			name:  "sha3-512",
			value: []byte("sha3-512"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			err := validateHashFunctionsParameter(ctx, nil, tt.value)
			require.NoError(t, err)
			require.False(t, vc.IsFatal())
		})
	}

	t.Run("wrong value", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		err := validateHashFunctionsParameter(ctx, nil, []byte("md8"))
		require.NoError(t, err)
		require.Equal(t, vc.Len(), 1)

		warn := vc.GetWarnings()[0]
		require.Equal(t, commonmodels.ValidationSeverityError, warn.Severity)
		require.Equal(t, "unknown hash function name", warn.Msg)
	})
}

func TestHashTransformer_Transform_multiple_iterations(t *testing.T) {
	// Check that internal buffers wipes correctly without data lost
	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)
	env := newTransformerTestEnvReal(t,
		HashTransformerDefinition,
		[]commonmodels.Column{
			{
				Idx:      0,
				Name:     "data",
				TypeName: "text",
				TypeOID:  23,
			},
		},
		map[string]commonmodels.ParamsValue{
			"column":   commonmodels.ParamsValue("data"),
			"function": commonmodels.ParamsValue("sha1"),
		},
		nil,
	)
	err := env.InitParameters(t, ctx)
	require.NoError(t, err)
	require.False(t, vc.HasWarnings())

	err = env.InitTransformer(t, ctx)
	require.NoError(t, err)
	require.False(t, vc.HasWarnings())

	tests := []struct {
		name     string
		original string
		expected string
		isNull   bool
	}{
		{
			name:     "run1",
			original: "123",
			expected: "40bd001563085fc35165329ea1ff5c5ecbdbbeef",
			isNull:   false,
		},
		{
			name:     "run2",
			original: "456",
			expected: "51eac6b471a284d3341d8c0c63d0f1a286262a18",
			isNull:   false,
		},
		{
			name:     "run3",
			original: "789",
			expected: "fc1200c7a7aa52109d762a9f005b149abef01479",
			isNull:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env.SetRecord(t, commonmodels.NewColumnRawValue([]byte(tt.original), tt.isNull))

			err = env.Transform(t, ctx)
			rec := env.GetRecord()
			val, err := rec.GetRawColumnValueByName("data")
			require.NoError(t, err)
			require.Equal(t, tt.isNull, val.IsNull)
		})
	}
}
