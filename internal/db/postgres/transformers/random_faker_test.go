package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomCCNumber_column_types(t *testing.T) {
	def, ok := utils.DefaultTransformerRegistry.Get(RandomCCNumberTransformerName)
	require.True(t, ok)

	// Card numbers have up to 19 digits and do not fit int4
	driver, _ := getDriverAndRecord("id4", "1")
	_, warnings, err := def.Instance(
		context.Background(), driver,
		map[string]toolkit.ParamsValue{"column": toolkit.ParamsValue("id4")},
		nil, "", false,
	)
	require.NoError(t, err)
	require.True(t, warnings.IsFatal())

	driver, _ = getDriverAndRecord("id8", "1")
	_, warnings, err = def.Instance(
		context.Background(), driver,
		map[string]toolkit.ParamsValue{"column": toolkit.ParamsValue("id8")},
		nil, "", false,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
}
