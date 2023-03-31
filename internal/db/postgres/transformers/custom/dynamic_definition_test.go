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

package custom

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestGetDynamicTransformerDefinition(t *testing.T) {
	expected := TransformerDefinition{
		Name:        "TwoDatesGen",
		Description: "Generate diff between two dates",
		Parameters: []*toolkit.Parameter{
			{
				Name:        "column_a",
				Description: "test1",
				Required:    true,
				IsColumn:    true,
				ColumnProperties: &toolkit.ColumnProperties{
					Affected:     true,
					AllowedTypes: []string{"date", "timestamp", "timestamptz"},
				},
			},
			{
				Name:        "column_b",
				Description: "test2",
				Required:    true,
				IsColumn:    true,
				ColumnProperties: &toolkit.ColumnProperties{
					Affected:     true,
					AllowedTypes: []string{"date", "timestamp", "timestamptz"},
				},
			},
		},
	}
	defStr := `{"name":"TwoDatesGen","description":"Generate diff between two dates","parameters":[{"name":"column_a","description":"test1","required":true,"is_column":true,"column_properties":{"affected":true,"allowed_types":["date","timestamp","timestamptz"]}},{"name":"column_b","description":"test2","required":true,"is_column":true,"column_properties":{"affected":true,"allowed_types":["date","timestamp","timestamptz"]}}]}`
	res, err := GetDynamicTransformerDefinition(context.Background(), "/bin/echo", defStr)
	require.NoError(t, err)
	require.Equal(t, expected.Name, res.Name)
	require.Equal(t, expected.Description, res.Description)
	require.Len(t, res.Parameters, 2)
	for idx, p := range res.Parameters {
		require.Equal(t, expected.Parameters[idx].Name, p.Name)
		require.Equal(t, expected.Parameters[idx].Description, p.Description)
		require.Equal(t, expected.Parameters[idx].IsColumn, p.IsColumn)
		require.Equal(t, expected.Parameters[idx].Required, p.Required)
		require.Equal(t, expected.Parameters[idx].ColumnProperties.Affected, p.ColumnProperties.Affected)
		require.Equal(t, expected.Parameters[idx].ColumnProperties.AllowedTypes, p.ColumnProperties.AllowedTypes)
	}
}
