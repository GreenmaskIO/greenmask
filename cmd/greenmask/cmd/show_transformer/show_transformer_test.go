package show_transformer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/utils/testutils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func fakeRegistry() *utils.TransformerRegistry {
	r := utils.NewTransformerRegistry()
	r.MustRegister(utils.NewTransformerDefinition(
		utils.NewTransformerProperties("Fake", "Generate a value that is long enough to be wrapped by the table"),
		nil,
		&toolkit.ParameterDefinition{
			Name: "column", Description: "column name", Required: true, IsColumn: true,
			ColumnProperties: &toolkit.ColumnProperties{
				AllowedTypes: []string{"date", "timestamp", "timestamptz"}, Affected: true, SkipOnNull: true,
			},
		},
		&toolkit.ParameterDefinition{
			Name: "min", Description: "min threshold", DefaultValue: toolkit.ParamsValue("1"),
			LinkColumnParameter: "column", CastDbType: "int8",
		},
		&toolkit.ParameterDefinition{
			Name: "max", Description: "max threshold", DefaultValue: toolkit.ParamsValue("1"),
			LinkColumnParameter: "column", CastDbType: "int8",
		},
		&toolkit.ParameterDefinition{Name: "columns", Description: "", IsColumnContainer: true},
	))
	return r
}

func TestShowTransformerText(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, showTransformerText(&out, fakeRegistry(), "Fake"))
	got := out.String()
	testutils.AssertGolden(t, "show_transformer.golden", got)

	cells := testutils.TableCells(got)
	assert.Equal(t, 1, testutils.CountCells(cells, 0, "Fake"))
	assert.Equal(t, 1, testutils.CountCells(cells, 1, "parameters"))
	// "min" and "max" have equal attributes, and each still gets its own rows.
	for _, name := range []string{"column", "min", "max", "columns"} {
		assert.Equalf(t, 1, testutils.CountCells(cells, 2, name), "parameter %s", name)
	}
	assert.Equal(t, 4, testutils.CountCells(cells, 3, "description"))
	assert.Equal(t, 4, testutils.CountCells(cells, 3, "required"))
	// Equal values in a column that is not merged stay on every row.
	assert.Equal(t, 2, testutils.CountCells(cells, 4, "column"))
	assert.Equal(t, 2, testutils.CountCells(cells, 4, "int8"))
	for _, v := range []string{"linked_parameter", "cast_to_db_type", "allowed_types", "is_affected",
		"skip_original_data", "skip_on_null", "timestamptz", "int8"} {
		assert.Contains(t, got, v)
	}
}

func TestShowTransformerText_unknown(t *testing.T) {
	var out bytes.Buffer
	require.Error(t, showTransformerText(&out, fakeRegistry(), "Unknown"))
	assert.Empty(t, out.String())
}
