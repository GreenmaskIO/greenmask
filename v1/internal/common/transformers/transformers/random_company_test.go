package transformers

import (
	"context"
	"slices"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomCompanyTransformer_Transform_static_fullname(t *testing.T) {

	columnName := "data"
	originalValue := "ACME Corp."
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .CompanyName }} {{ .CompanySuffix }}"}]`),
		"engine":  toolkit.ParamsValue("random"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomCompany")
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
	require.True(t, testStringContainsOneOfItemFromList(string(rawVal.Data), transformers.DefaultCompanyNames))
}

func TestRandomCompanyTransformer_Transform_static_Suffix(t *testing.T) {

	columnName := "data"
	originalValue := "ACME Corp."
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .CompanySuffix }}"}]`),
		"engine":  toolkit.ParamsValue("random"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomCompany")
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
	suffixes := transformers.DefaultCompanySuffixes
	require.True(t, slices.Contains(suffixes, string(rawVal.Data)))
}

func TestRandomCompanyTransformer_Transform_static_CompanyName(t *testing.T) {

	columnName := "data"
	originalValue := "ACME Corp."
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .CompanyName }}"}]`),
		"engine":  toolkit.ParamsValue("random"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomCompany")
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
	require.True(t, slices.Contains(transformers.DefaultCompanyNames, string(rawVal.Data)))
}

func TestRandomCompanyTransformer_Transform_static_nullable(t *testing.T) {
	columnName := "data"
	originalValue := "\\N"
	params := map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .CompanyName }} {{ .CompanySuffix }}"}]`),
		"engine":  toolkit.ParamsValue("hash"),
	}

	driver, record := getDriverAndRecord(columnName, originalValue)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomCompany")
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
