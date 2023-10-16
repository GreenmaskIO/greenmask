package custom

import (
	"context"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

func TestGetDynamicTransformerDefinition(t *testing.T) {
	expected := CustomTransformerDefinition{
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
