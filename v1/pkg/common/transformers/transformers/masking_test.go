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

	"github.com/google/go-cmp/cmp"
	"github.com/greenmaskio/greenmask/v1/pkg/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaskingTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		columnName       string
		staticParameters map[string]models.ParamsValue
		dynamicParameter map[string]models.DynamicParamValue
		original         *models.ColumnRawValue
		expected         *models.ColumnRawValue
		validateFn       func(t *testing.T, expected, actual *models.ColumnRawValue)
		expectedErr      string
		columns          []models.Column
		isNull           bool
	}{
		{
			name: MMobile,
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"type":   models.ParamsValue(MMobile),
			},
			original: models.NewColumnRawValue([]byte("+35798665784"), false),
			expected: models.NewColumnRawValue(
				[]byte("+357***65784"),
				false,
			),
			isNull:     false,
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeText,
					TypeOID:   mysqldbmsdriver.VirtualOidText,
					TypeClass: models.TypeClassText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				diff := cmp.Diff(expected, actual)
				if diff != "" {
					t.Errorf("mismatch (-expected +actual):\n%s", diff)
				}
			},
		},
		{
			name: MName,
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"type":   models.ParamsValue(MName),
			},
			original:   models.NewColumnRawValue([]byte("abcdef test"), false),
			expected:   models.NewColumnRawValue([]byte("a**def t**t"), false),
			isNull:     false,
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeText,
					TypeOID:   mysqldbmsdriver.VirtualOidText,
					TypeClass: models.TypeClassText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				diff := cmp.Diff(expected, actual)
				if diff != "" {
					t.Errorf("mismatch (-expected +actual):\n%s", diff)
				}
			},
		},
		{
			name: MPassword,
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"type":   models.ParamsValue(MPassword),
			},
			original:   models.NewColumnRawValue([]byte("password_secure"), false),
			expected:   models.NewColumnRawValue([]byte("************"), false),
			isNull:     false,
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeText,
					TypeOID:   mysqldbmsdriver.VirtualOidText,
					TypeClass: models.TypeClassText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				diff := cmp.Diff(expected, actual)
				if diff != "" {
					t.Errorf("mismatch (-expected +actual):\n%s", diff)
				}
			},
		},
		{
			name: MDefault,
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"type":   models.ParamsValue(MDefault),
			},
			original:   models.NewColumnRawValue([]byte("123"), false),
			expected:   models.NewColumnRawValue([]byte("***"), false),
			isNull:     false,
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeText,
					TypeOID:   mysqldbmsdriver.VirtualOidText,
					TypeClass: models.TypeClassText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				diff := cmp.Diff(expected, actual)
				if diff != "" {
					t.Errorf("mismatch (-expected +actual):\n%s", diff)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				MaskingTransformerDefinition,
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

			env.SetRecord(t, tt.original)

			err = env.Transform(t, ctx)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			rec := env.GetRecord()
			actual, err := rec.GetRawColumnValueByName(tt.columnName)
			require.NoError(t, err)
			tt.validateFn(t, tt.expected, actual)
		})
	}
}

func TestNewMaskingTransformer(t *testing.T) {
	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)
	env := newTransformerTestEnvReal(t,
		MaskingTransformerDefinition,
		[]models.Column{
			{
				Idx:       0,
				Name:      "data",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: models.TypeClassText,
			},
		},
		map[string]models.ParamsValue{
			"column": models.ParamsValue("data"),
			"type":   models.ParamsValue("test"),
		},
		nil,
	)
	err := env.InitParameters(t, ctx)
	require.ErrorIs(t, err, models.ErrFatalValidationError)
	require.Equal(t, vc.Len(), 1)
	assert.Contains(t, vc.GetWarnings()[0].Msg, "unknown masking type")
}
