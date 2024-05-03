package transformers

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

func TestRandomIntTransformer_Transform_static_fullname(t *testing.T) {

	columnName := "data"
	originalValue := "John Dust123"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "part": "full_name"}]`),
		"engine":  toolkit.ParamsValue("random"),
		"gender":  toolkit.ParamsValue("any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomFullName")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	rawVal, err := r.GetRawColumnValueByName(columnName)
	require.NoError(t, err)
	require.False(t, rawVal.IsNull)
	require.True(t, testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultFirstNamesFemale) || testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultFirstNamesMale))
	require.True(t, testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultLastNames))
}

func TestRandomIntTransformer_Transform_static_firstname(t *testing.T) {

	columnName := "data"
	originalValue := "John Dust123"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "part": "first_name"}]`),
		"engine":  toolkit.ParamsValue("random"),
		"gender":  toolkit.ParamsValue("any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomFullName")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	rawVal, err := r.GetRawColumnValueByName(columnName)
	require.NoError(t, err)
	require.False(t, rawVal.IsNull)
	totalNames := append(transformers.DefaultFirstNamesFemale, transformers.DefaultFirstNamesMale...)
	require.True(t, slices.Contains(totalNames, string(rawVal.Data)))
}

func TestRandomIntTransformer_Transform_static_lastname(t *testing.T) {

	columnName := "data"
	originalValue := "John Dust123"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "part": "last_name"}]`),
		"engine":  toolkit.ParamsValue("random"),
		"gender":  toolkit.ParamsValue("any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomFullName")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	rawVal, err := r.GetRawColumnValueByName(columnName)
	require.NoError(t, err)
	require.False(t, rawVal.IsNull)
	require.True(t, slices.Contains(transformers.DefaultLastNames, string(rawVal.Data)))
}

func TestRandomIntTransformer_Transform_static_template(t *testing.T) {

	columnName := "data"
	originalValue := "John Dust123"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .LastName }} {{ .FirstName }} 123" }]`),
		"engine":  toolkit.ParamsValue("random"),
		"gender":  toolkit.ParamsValue("any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomFullName")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	rawVal, err := r.GetRawColumnValueByName(columnName)
	require.NoError(t, err)
	require.False(t, rawVal.IsNull)
	require.True(t, testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultFirstNamesFemale) || testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultFirstNamesMale))
	require.True(t, testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultLastNames))
}

func testStringContainsOneOfItemFromList(val string, values []string) bool {
	for _, item := range values {
		if strings.Contains(val, item) {
			return true
		}
	}
	return false
}
