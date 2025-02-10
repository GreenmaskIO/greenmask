package transformers

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomPersonTransformer_Transform_static_fullname(t *testing.T) {

	columnName := "data"
	originalValue := "John Dust123"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .Title }} {{ .FirstName }} {{ .LastName }}"}]`),
		"engine":  toolkit.ParamsValue("random"),
		"gender":  toolkit.ParamsValue("Any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
		"",
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
	log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
	require.True(t, testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultFirstNamesFemale) || testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultFirstNamesMale))
	require.True(t, testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultLastNames))
}

func TestRandomPersonTransformer_Transform_static_firstname(t *testing.T) {

	columnName := "data"
	originalValue := "John Dust123"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .FirstName }}"}]`),
		"engine":  toolkit.ParamsValue("random"),
		"gender":  toolkit.ParamsValue("Any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
		"",
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
	log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
	totalNames := append(transformers.DefaultFirstNamesFemale, transformers.DefaultFirstNamesMale...)
	require.True(t, slices.Contains(totalNames, string(rawVal.Data)))
}

func TestRandomPersonTransformer_Transform_static_lastname(t *testing.T) {

	columnName := "data"
	originalValue := "John Dust123"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .LastName }}"}]`),
		"engine":  toolkit.ParamsValue("random"),
		"gender":  toolkit.ParamsValue("Any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
		"",
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
	log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
	require.True(t, slices.Contains(transformers.DefaultLastNames, string(rawVal.Data)))
}

func testStringContainsOneOfItemFromList(val string, values []string) bool {
	for _, item := range values {
		if strings.Contains(val, item) {
			return true
		}
	}
	return false
}

func TestRandomPersonTransformer_Transform_static_nullable(t *testing.T) {
	columnName := "data"
	originalValue := "\\N"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .Title }} {{ .FirstName }} {{ .LastName }}"}]`),
		"engine":  toolkit.ParamsValue("hash"),
		"gender":  toolkit.ParamsValue("Any"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
	require.True(t, ok)

	transformer, warnings, err := def.Instance(
		context.Background(),
		driver,
		params,
		nil,
		"",
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
	require.True(t, rawVal.IsNull)
}
