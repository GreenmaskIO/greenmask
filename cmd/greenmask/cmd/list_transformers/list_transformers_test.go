package list_transformers

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/utils/testutils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func fakeRegistry() *utils.TransformerRegistry {
	r := utils.NewTransformerRegistry()
	r.MustRegister(utils.NewTransformerDefinition(
		utils.NewTransformerProperties("Alpha", "Generate a value that is long enough to be wrapped by the table"),
		nil,
		&toolkit.ParameterDefinition{Name: "column", IsColumn: true},
		&toolkit.ParameterDefinition{Name: "min"},
		&toolkit.ParameterDefinition{
			Name: "linked_column", IsColumn: true,
			ColumnProperties: &toolkit.ColumnProperties{AllowedTypes: []string{"date", "timestamp", "timestamptz"}},
		},
	))
	r.MustRegister(utils.NewTransformerDefinition(
		utils.NewTransformerProperties("Beta", ""),
		nil,
		&toolkit.ParameterDefinition{
			Name: "column", IsColumn: true,
			ColumnProperties: &toolkit.ColumnProperties{AllowedTypes: []string{
				"text", "varchar", "char", "bpchar", "citext", "int2", "int4", "int8", "numeric",
			}},
		},
	))
	r.MustRegister(utils.NewTransformerDefinition(
		utils.NewTransformerProperties("Gamma", ""),
		nil,
		&toolkit.ParameterDefinition{Name: "columns", IsColumnContainer: true},
	))
	return r
}

func TestListTransformersText(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, listTransformersText(&out, fakeRegistry()))
	got := out.String()
	testutils.AssertGolden(t, "list_transformers.golden", got)

	cells := testutils.TableCells(got)
	for _, name := range []string{"Alpha", "Beta", "Gamma"} {
		assert.Equalf(t, 1, testutils.CountCells(cells, 0, name), "transformer %s", name)
	}
	// Each column parameter is a row, and equal names of different transformers are not merged.
	assert.Equal(t, 2, testutils.CountCells(cells, 2, "column"))
	assert.Equal(t, 1, testutils.CountCells(cells, 2, "linked_column"))
	assert.Equal(t, 1, testutils.CountCells(cells, 2, "columns"))
	assert.Equal(t, 0, testutils.CountCells(cells, 2, "min"))
	assert.Equal(t, 2, testutils.CountCells(cells, 3, "any"))
	assert.Contains(t, got, "numeric")
	assert.Contains(t, got, "timestamptz")
}

func TestListTransformersText_default_registry(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, listTransformersText(&out, utils.DefaultTransformerRegistry))
	got := out.String()
	for name, def := range utils.DefaultTransformerRegistry.M {
		for _, p := range def.Parameters {
			if p.IsColumn || p.IsColumnContainer {
				assert.Containsf(t, got, " "+p.Name+" ", "%s.%s", name, p.Name)
			}
		}
	}
}
