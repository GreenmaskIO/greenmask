package validate_utils

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func appendRows(t *testing.T, doc Documenter, tab *entries.Table, original, transformed [][]byte) {
	t.Helper()
	originalRow := pgcopy.NewRow(len(tab.Columns))
	transformedRow := pgcopy.NewRow(len(tab.Columns))
	for idx := range original {
		require.NoErrorf(t, originalRow.Decode(original[idx]), "error at %d line", idx)
		require.NoErrorf(t, transformedRow.Decode(transformed[idx]), "error at %d line", idx)
		require.NoErrorf(t, doc.Append(originalRow, transformedRow), "error at %d line", idx)
	}
}

func summaryByName(columns []columnSummary) map[string]columnSummary {
	res := make(map[string]columnSummary, len(columns))
	for _, c := range columns {
		res[c.Name] = c
	}
	return res
}

func TestSummaryDocument_Counts(t *testing.T) {
	tab, original, transformed := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50, nil)
	appendRows(t, sd, tab, original, transformed)

	columns := summaryByName(sd.summarize())
	require.Len(t, columns, 4)

	// "name" is the transformed column and changes in every row.
	assert.True(t, columns["name"].Transformed)
	assert.Equal(t, uint64(6), columns["name"].Changed)
	assert.Equal(t, uint64(0), columns["name"].Unchanged)
	assert.Empty(t, columns["name"].Warning)

	// "groupname" has no transformer and stays the same.
	assert.False(t, columns["groupname"].Transformed)
	assert.Equal(t, uint64(6), columns["groupname"].Unchanged)
	assert.Empty(t, columns["groupname"].Warning)

	// "modifieddate" became NULL in one row without a transformer.
	assert.Equal(t, uint64(1), columns["modifieddate"].Changed)
	assert.Equal(t, unexpectedWarning, columns["modifieddate"].Warning)
	assert.Len(t, sd.Warnings(), 1)
}

func TestSummaryDocument_UnchangedThreshold(t *testing.T) {
	tab, original, _ := getTableAndRows()
	// "name" keeps 3 of its 6 values: exactly 50%.
	transformed := [][]byte{
		[]byte("1\ttes1\tResearch and Development\t2008-04-30 00:00:00"),
		[]byte("2\ttes2\tResearch and Development\t2008-04-30 00:00:00"),
		[]byte("3\ttes3\tSales and Marketing\t2008-04-30 00:00:00"),
		[]byte("4\tMarketing\tSales and Marketing\t2008-04-30 00:00:00"),
		[]byte("5\tPurchasing\tInventory Management\t2008-04-30 00:00:00"),
		[]byte("6\tResearch and Development\tResearch and Development\t2008-04-30 00:00:00"),
	}

	tests := []struct {
		name      string
		threshold float64
		warns     bool
	}{
		{name: "0 warns on any unchanged value", threshold: 0, warns: true},
		{name: "below the ratio warns", threshold: 49.9, warns: true},
		{name: "exactly on the ratio does not warn", threshold: 50, warns: false},
		{name: "100 turns the warning off", threshold: 100, warns: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sd := NewSummaryDocument(tab, jsonFormatName, false, tt.threshold, nil)
			appendRows(t, sd, tab, original, transformed)
			name := summaryByName(sd.summarize())["name"]
			assert.InDelta(t, 0.5, name.UnchangedRatio, 1e-9)
			if tt.warns {
				assert.Equal(t, unchangedWarning, name.Warning)
			} else {
				assert.Empty(t, name.Warning)
			}
		})
	}
}

func TestSummaryDocument_NullsKeptDoNotCountAsUnchanged(t *testing.T) {
	tab, _, _ := getTableAndRows()
	original := [][]byte{
		[]byte("1\t\\N\tA\t2008-04-30 00:00:00"),
		[]byte("2\t\\N\tB\t2008-04-30 00:00:00"),
		[]byte("3\tSales\tC\t2008-04-30 00:00:00"),
	}
	transformed := [][]byte{
		[]byte("1\t\\N\tA\t2008-04-30 00:00:00"),
		[]byte("2\t\\N\tB\t2008-04-30 00:00:00"),
		[]byte("3\ttes3\tC\t2008-04-30 00:00:00"),
	}

	sd := NewSummaryDocument(tab, jsonFormatName, false, 0, nil)
	appendRows(t, sd, tab, original, transformed)
	name := summaryByName(sd.summarize())["name"]
	assert.Equal(t, uint64(2), name.NullsKept)
	assert.Equal(t, uint64(1), name.Changed)
	assert.Zero(t, name.UnchangedRatio)
	assert.Empty(t, name.Warning)
}

func TestSummaryDocument_OnlyTransformed(t *testing.T) {
	tab, original, transformed := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, true, 50, nil)
	appendRows(t, sd, tab, original, transformed)

	// "name" is transformed; "modifieddate" is kept because it has a warning.
	columns := summaryByName(sd.visible(sd.summarize()))
	assert.Len(t, columns, 2)
	assert.Contains(t, columns, "name")
	assert.Contains(t, columns, "modifieddate")
}

// Every value is a distinct sentinel, so any leak of a value or a primary key shows up as a match.
func TestSummaryDocument_PrintsNoValues(t *testing.T) {
	tab, _, _ := getTableAndRows()
	original := [][]byte{
		[]byte("910001\tsecret-orig-name-1\tsecret-orig-group-1\t1999-01-01 01:01:01"),
		[]byte("910002\tsecret-orig-name-2\tsecret-orig-group-2\t1999-02-02 02:02:02"),
	}
	transformed := [][]byte{
		[]byte("910001\tsecret-trans-name-1\tsecret-orig-group-1\t1999-01-01 01:01:01"),
		[]byte("910002\tsecret-trans-name-2\tsecret-orig-group-2\t1999-02-02 02:02:02"),
	}
	sentinels := []string{
		"910001", "910002", "secret-", "1999-01-01", "1999-02-02",
	}

	for _, format := range []string{jsonFormatName, "text"} {
		t.Run(format, func(t *testing.T) {
			sd := NewSummaryDocument(tab, format, false, 50, nil)
			appendRows(t, sd, tab, original, transformed)
			var out bytes.Buffer
			require.NoError(t, sd.Print(&out))
			require.Contains(t, out.String(), "groupname")
			for _, v := range sentinels {
				assert.NotContains(t, out.String(), v)
			}
		})
	}
}

func TestSummaryDocument_JsonShape(t *testing.T) {
	tab, original, transformed := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50, nil)
	appendRows(t, sd, tab, original, transformed)
	var out bytes.Buffer
	require.NoError(t, sd.Print(&out))

	var res summaryDocumentResponse
	require.NoError(t, json.Unmarshal(out.Bytes(), &res))
	assert.Equal(t, "humanresources", res.Schema)
	assert.Equal(t, "department", res.Name)
	assert.Equal(t, summaryDiffModeName, res.DiffMode)
	assert.Equal(t, uint64(6), res.Rows)
	require.Len(t, res.Columns, 4)
	assert.Equal(t, "departmentid", res.Columns[0].Name)
}

func TestSummaryDocument_NullTransitionsCountAsChanged(t *testing.T) {
	tab, _, _ := getTableAndRows()
	original := [][]byte{
		[]byte("1\t\\N\tA\t2008-04-30 00:00:00"),
		[]byte("2\tSales\tB\t2008-04-30 00:00:00"),
	}
	transformed := [][]byte{
		[]byte("1\ttes1\tA\t2008-04-30 00:00:00"),
		[]byte("2\t\\N\tB\t2008-04-30 00:00:00"),
	}

	sd := NewSummaryDocument(tab, jsonFormatName, false, 50, nil)
	appendRows(t, sd, tab, original, transformed)
	name := summaryByName(sd.summarize())["name"]
	assert.Equal(t, uint64(2), name.Changed)
	assert.Zero(t, name.NullsKept)
}

func TestSummaryDocument_EmptyTable(t *testing.T) {
	tab, _, _ := getTableAndRows()
	for _, format := range []string{jsonFormatName, "text"} {
		t.Run(format, func(t *testing.T) {
			sd := NewSummaryDocument(tab, format, false, 0, nil)
			for _, c := range sd.summarize() {
				assert.Zero(t, c.UnchangedRatio)
				assert.Empty(t, c.Warning)
			}
			assert.Empty(t, sd.Warnings())
			var out bytes.Buffer
			require.NoError(t, sd.Print(&out))
			assert.NotEmpty(t, out.String())
		})
	}
}

func TestSummaryDocument_ColumnCountMismatch(t *testing.T) {
	tab, _, _ := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50, nil)
	short := pgcopy.NewRow(2)
	require.NoError(t, short.Decode([]byte("1\tA")))
	assert.Error(t, sd.Append(short, short))
}

func TestSummaryDocument_WarningHash(t *testing.T) {
	tab, original, transformed := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50, nil)
	appendRows(t, sd, tab, original, transformed)

	warns := sd.Warnings()
	require.Len(t, warns, 1)
	w := warns[0]
	assert.Equal(t, toolkit.UntransformedColumnChangedWarningType, w.Meta["WarningType"])
	assert.Equal(t, "modifieddate", w.Meta["ColumnName"])
	assert.Equal(t, summaryByName(sd.summarize())["modifieddate"].Hash, w.Hash)

	// More rows change the counts but not the hash, so a resolved warning stays resolved.
	appendRows(t, sd, tab, original, transformed)
	assert.Equal(t, w.Hash, sd.Warnings()[0].Hash)
	assert.False(t, sd.Warnings().HasUnresolved([]string{w.Hash}))
}

func TestSummaryDocument_ResolvedWarningIsExcluded(t *testing.T) {
	tab, original, transformed := getTableAndRows()
	// "name" keeps 3 of 6 values, so with threshold 0 it warns next to "modifieddate".
	transformed[0], transformed[1], transformed[2] = original[0], original[1], original[2]
	probe := NewSummaryDocument(tab, jsonFormatName, false, 0, nil)
	appendRows(t, probe, tab, original, transformed)
	byName := summaryByName(probe.summarize())
	require.NotEmpty(t, byName["name"].Hash)
	require.NotEmpty(t, byName["modifieddate"].Hash)
	resolved := []string{byName["modifieddate"].Hash}

	t.Run("dropped from summary and warnings", func(t *testing.T) {
		sd := NewSummaryDocument(tab, jsonFormatName, false, 0, resolved)
		appendRows(t, sd, tab, original, transformed)
		columns := summaryByName(sd.summarize())
		assert.Empty(t, columns["modifieddate"].Warning)
		assert.Empty(t, columns["modifieddate"].Hash)
		assert.Equal(t, uint64(1), columns["modifieddate"].Changed)
		assert.Equal(t, unchangedWarning, columns["name"].Warning)
		warns := sd.Warnings()
		require.Len(t, warns, 1)
		assert.Equal(t, "name", warns[0].Meta["ColumnName"])
	})

	t.Run("dropped from output", func(t *testing.T) {
		for _, format := range []string{jsonFormatName, "text"} {
			sd := NewSummaryDocument(tab, format, false, 0, resolved)
			appendRows(t, sd, tab, original, transformed)
			var out bytes.Buffer
			require.NoError(t, sd.Print(&out))
			assert.NotContains(t, out.String(), unexpectedWarning, format)
			assert.NotContains(t, out.String(), resolved[0], format)
			assert.Contains(t, out.String(), unchangedWarning, format)
		}
	})

	t.Run("transformed-only hides the untransformed column", func(t *testing.T) {
		sd := NewSummaryDocument(tab, jsonFormatName, true, 0, resolved)
		appendRows(t, sd, tab, original, transformed)
		columns := summaryByName(sd.visible(sd.summarize()))
		assert.NotContains(t, columns, "modifieddate")
		assert.Contains(t, columns, "name")
	})
}

func TestSummaryDocument_SummaryIsCached(t *testing.T) {
	tab, original, transformed := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50, nil)
	appendRows(t, sd, tab, original, transformed)

	var out bytes.Buffer
	require.NoError(t, sd.Print(&out))
	warns := sd.Warnings()
	require.Len(t, warns, 1)
	assert.Same(t, summaryByName(sd.summary)["modifieddate"].warning, warns[0])

	// A new row drops the cache, so the counts include it.
	appendRows(t, sd, tab, original[:1], transformed[:1])
	assert.Equal(t, uint64(7), summaryByName(sd.summaries())["groupname"].Unchanged)
}
