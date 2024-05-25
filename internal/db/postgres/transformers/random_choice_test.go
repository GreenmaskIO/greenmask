package transformers

import (
	"context"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomChoiceTransformer_Transform_with_fail(t *testing.T) {

	original := "2023-11-10"

	params := map[string]toolkit.ParamsValue{
		"column":   toolkit.ParamsValue("date_date"),
		"values":   toolkit.ParamsValue(`["2023-11-10", "2023-01-01", "2023-01-02"]`),
		"validate": toolkit.ParamsValue(`true`),
	}

	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformerCtx, warnings, err := ChoiceTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
		"",
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
	r, err := transformerCtx.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	res, err := r.GetRawColumnValueByName(string(params["column"]))
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	val := string(res.Data)
	require.True(t, val == "2023-11-10" || val == "2023-01-01" || val == "2023-01-02")
}

func TestRandomChoiceTransformer_Transform_validation_error(t *testing.T) {

	original := "2023-11-10"

	params := map[string]toolkit.ParamsValue{
		"column":   toolkit.ParamsValue("date_date"),
		"values":   toolkit.ParamsValue(`["value_error"]`),
		"validate": toolkit.ParamsValue(`true`),
	}

	driver, _ := getDriverAndRecord(string(params["column"]), original)
	_, warnings, err := ChoiceTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
		"",
	)
	require.NoError(t, err)
	require.NotEmpty(t, warnings)
	require.True(t, warnings.IsFatal())
}

func TestRandomChoiceTransformer_Transform_json(t *testing.T) {

	original := `{"f": 4}`

	params := map[string]toolkit.ParamsValue{
		"column":   toolkit.ParamsValue("doc"),
		"values":   toolkit.ParamsValue(`[{"a": 1}, {"b": 2}, {"c": 3}]`),
		"validate": toolkit.ParamsValue(`true`),
	}

	driver, record := getDriverAndRecord(string(params["column"]), original)
	transformerCtx, warnings, err := ChoiceTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
		"",
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
	r, err := transformerCtx.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)

	res, err := r.GetRawColumnValueByName(string(params["column"]))
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	val := string(res.Data)
	log.Debug().Msg(val)
	require.True(t, val == `{"a": 1}` || val == `{"b": 2}` || val == `{"c": 3}`)
}
