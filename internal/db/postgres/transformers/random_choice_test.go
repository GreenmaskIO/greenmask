package transformers

import (
	"context"
	"testing"

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
	transformer, warnings, err := RandomChoiceTransformerDefinition.Instance(
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

	res, err := r.GetRawAttributeValueByName(string(params["column"]))
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
	_, warnings, err := RandomChoiceTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "error validating value")
	require.Empty(t, warnings)
}
