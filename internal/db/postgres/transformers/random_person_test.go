package transformers

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
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

func TestRandomPersonTransformer_Transform_keep_null(t *testing.T) {
	t.Run("keep_null for all columns", func(t *testing.T) {
		originalValue := "\\N\t\\N"
		params := map[string]toolkit.ParamsValue{
			"columns": toolkit.ParamsValue(`
			[
				{
					"name": "first_name", 
					"template": "{{ .FirstName }}",
					"keep_null": true
				},
				{
					"name": "last_name", 
					"template": "{{ .LastName }}",
					"keep_null": true
				}
			]`,
			),
			"engine": toolkit.ParamsValue("hash"),
			"gender": toolkit.ParamsValue("Any"),
		}

		driver, record := getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)
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

		fistNameRawValue, err := r.GetRawColumnValueByName("first_name")
		require.NoError(t, err)
		require.True(t, fistNameRawValue.IsNull)

		lastNameRawValue, err := r.GetRawColumnValueByName("last_name")
		require.NoError(t, err)
		require.True(t, lastNameRawValue.IsNull)
	})

	t.Run("keep_null only for one column", func(t *testing.T) {
		originalValue := "\\N\t\\N"
		params := map[string]toolkit.ParamsValue{
			"columns": toolkit.ParamsValue(`
			[
				{
					"name": "first_name", 
					"template": "{{ .FirstName }}",
					"keep_null": false
				},
				{
					"name": "last_name", 
					"template": "{{ .LastName }}",
					"keep_null": true
				}
			]`,
			),
			"engine": toolkit.ParamsValue("hash"),
			"gender": toolkit.ParamsValue("Any"),
		}

		driver, record := getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)
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

		fistNameRawValue, err := r.GetRawColumnValueByName("first_name")
		require.NoError(t, err)
		require.False(t, fistNameRawValue.IsNull)

		lastNameRawValue, err := r.GetRawColumnValueByName("last_name")
		require.NoError(t, err)
		require.True(t, lastNameRawValue.IsNull)
	})

	t.Run("keep_null false for all columns and hash two times", func(t *testing.T) {
		originalValue := "\\N\t\\N"
		params := map[string]toolkit.ParamsValue{
			"columns": toolkit.ParamsValue(`
			[
				{
					"name": "first_name", 
					"template": "{{ .FirstName }}",
					"keep_null": false,
					"hashing": true
				},
				{
					"name": "last_name", 
					"template": "{{ .LastName }}",
					"keep_null": false,
					"hashing": true
				}
			]`,
			),
			"engine": toolkit.ParamsValue("hash"),
			"gender": toolkit.ParamsValue("Any"),
		}

		driver, record := getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)
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

		fistNameRawValue, err := r.GetRawColumnValueByName("first_name")
		require.NoError(t, err)
		require.False(t, fistNameRawValue.IsNull)
		fistName := string(fistNameRawValue.Data)

		lastNameRawValue, err := r.GetRawColumnValueByName("last_name")
		require.NoError(t, err)
		require.False(t, lastNameRawValue.IsNull)
		lastName := string(lastNameRawValue.Data)

		driver, record = getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)

		r, err = transformer.Transformer.Transform(
			context.Background(),
			record,
		)
		require.NoError(t, err)

		fistNameRawValue, err = r.GetRawColumnValueByName("first_name")
		require.NoError(t, err)
		require.False(t, fistNameRawValue.IsNull)
		assert.Equal(t, fistName, string(fistNameRawValue.Data))

		lastNameRawValue, err = r.GetRawColumnValueByName("last_name")
		require.NoError(t, err)
		require.False(t, lastNameRawValue.IsNull)
		assert.Equal(t, lastName, string(lastNameRawValue.Data))
	})
}
