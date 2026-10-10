package validate_utils

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
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
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50)
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
			sd := NewSummaryDocument(tab, jsonFormatName, false, tt.threshold)
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

	sd := NewSummaryDocument(tab, jsonFormatName, false, 0)
	appendRows(t, sd, tab, original, transformed)
	name := summaryByName(sd.summarize())["name"]
	assert.Equal(t, uint64(2), name.NullsKept)
	assert.Equal(t, uint64(1), name.Changed)
	assert.Zero(t, name.UnchangedRatio)
	assert.Empty(t, name.Warning)
}

func TestSummaryDocument_OnlyTransformed(t *testing.T) {
	tab, original, transformed := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, true, 50)
	appendRows(t, sd, tab, original, transformed)

	// "name" is transformed; "modifieddate" is kept because it has a warning.
	columns := summaryByName(sd.visible(sd.summarize()))
	assert.Len(t, columns, 2)
	assert.Contains(t, columns, "name")
	assert.Contains(t, columns, "modifieddate")
}

// The point of the summary mode: no value from either side, and no primary
// key, reaches the output. Every value here is a distinct sentinel, so any
// leak shows up as an exact match.
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
			sd := NewSummaryDocument(tab, format, false, 50)
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
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50)
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

	sd := NewSummaryDocument(tab, jsonFormatName, false, 50)
	appendRows(t, sd, tab, original, transformed)
	name := summaryByName(sd.summarize())["name"]
	assert.Equal(t, uint64(2), name.Changed)
	assert.Zero(t, name.NullsKept)
}

func TestSummaryDocument_EmptyTable(t *testing.T) {
	tab, _, _ := getTableAndRows()
	for _, format := range []string{jsonFormatName, "text"} {
		t.Run(format, func(t *testing.T) {
			sd := NewSummaryDocument(tab, format, false, 0)
			for _, c := range sd.summarize() {
				assert.Zero(t, c.UnchangedRatio)
				assert.Empty(t, c.Warning)
			}
			var out bytes.Buffer
			require.NoError(t, sd.Print(&out))
			assert.NotEmpty(t, out.String())
		})
	}
}

func TestSummaryDocument_ColumnCountMismatch(t *testing.T) {
	tab, _, _ := getTableAndRows()
	sd := NewSummaryDocument(tab, jsonFormatName, false, 50)
	short := pgcopy.NewRow(2)
	require.NoError(t, short.Decode([]byte("1\tA")))
	assert.Error(t, sd.Append(short, short))
}
