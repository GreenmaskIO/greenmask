package validate_utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/utils/testutils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type affectedColumnsTransformer struct {
	columns map[int]string
}

func (tt *affectedColumnsTransformer) Init(context.Context) error { return nil }

func (tt *affectedColumnsTransformer) Done(context.Context) error { return nil }

func (tt *affectedColumnsTransformer) Transform(context.Context, *toolkit.Record) (*toolkit.Record, error) {
	return nil, nil
}

func (tt *affectedColumnsTransformer) GetAffectedColumns() map[int]string { return tt.columns }

const (
	longWithSpaces   = "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore"
	longWithoutSpace = "https://example.com/very/long/path/without/any/spaces/that/is/longer/than/sixty/four/bytes"
	longCyrillic     = "Съешь же ещё этих мягких французских булок да выпей чаю и снова съешь этих булок"
	longCyrillicWord = "Длинноесловобезпробеловкотороедлиннеечемшестьдесятчетыресимволаибайта"
)

// textDocumentTable has a column with "_" in its name, a transformed column with long values, a column with equal
// values in adjacent rows, and a column that changes without a transformer.
func textDocumentTable(t *testing.T) (*entries.Table, [][]byte, [][]byte) {
	t.Helper()
	columns := []*toolkit.Column{
		{Name: "id", TypeName: "integer", TypeOid: 23, Num: 1, NotNull: true, Length: -1},
		{Name: "first_name", TypeName: "text", TypeOid: 25, Num: 2, Length: -1},
		{Name: "group_name", TypeName: "text", TypeOid: 25, Num: 3, Length: -1},
		{Name: "bio", TypeName: "text", TypeOid: 25, Num: 4, Length: -1},
		{Name: "note", TypeName: "text", TypeOid: 25, Num: 5, Length: -1},
	}
	pk := &toolkit.PrimaryKey{}
	require.NoError(t, json.Unmarshal([]byte(`{"schema": "public", "name": "people_pkey", "columns": [1]}`), pk))
	tab := &entries.Table{
		Table: &toolkit.Table{
			Schema:      "public",
			Name:        "people",
			Columns:     columns,
			Constraints: []toolkit.Constraint{pk},
		},
		TransformersContext: []*utils.TransformerContext{
			{Transformer: &affectedColumnsTransformer{columns: map[int]string{1: "first_name", 3: "bio"}}},
		},
	}
	row := func(values ...string) []byte {
		return []byte(strings.Join(values, "\t"))
	}
	original := [][]byte{
		row("1", "John", "Sales", longWithSpaces, ""),
		row("2", "Anna", "Sales", longWithoutSpace, `\N`),
		row("3", "Ivan", "Sales", longCyrillic, "keep"),
	}
	transformed := [][]byte{
		row("1", "Mark", "Sales", "Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip", ""),
		row("2", "Anna", "Sales", longWithoutSpace, `\N`),
		row("3", `\N`, "Sales", longCyrillicWord, "changed"),
	}
	return tab, original, transformed
}

func TestTextDocument_Print(t *testing.T) {
	// Colors are checked by the .colors.golden files whatever NO_COLOR or TERM the tests run with.
	text.EnableColors()
	tests := []struct {
		name            string
		format          string
		withDiff        bool
		onlyTransformed bool
		// columns is the expected column order of the horizontal header, or of the rows in vertical.
		columns []string
		// salesLines is how many table lines hold a "Sales" cell: equal values in a non-merged column stay.
		salesLines int
	}{
		{
			name: "vertical", format: verticalTableFormatName,
			columns:    []string{"id", "first_name", "group_name", "bio", "note"},
			salesLines: 3,
		},
		{
			name: "vertical_diff", format: verticalTableFormatName, withDiff: true,
			columns:    []string{"id", "first_name", "group_name", "bio", "note"},
			salesLines: 3,
		},
		{
			name: "vertical_only_transformed", format: verticalTableFormatName, onlyTransformed: true,
			columns: []string{"id", "first_name", "bio", "note"},
		},
		{
			name: "vertical_diff_only_transformed", format: verticalTableFormatName, withDiff: true, onlyTransformed: true,
			columns: []string{"id", "first_name", "bio", "note"},
		},
		{
			name: "horizontal", format: horizontalTableFormatName,
			columns:    []string{"%LineNum%", "id", "first_name", "group_name", "bio", "note (!!!)"},
			salesLines: 3,
		},
		{
			name: "horizontal_diff", format: horizontalTableFormatName, withDiff: true,
			columns:    []string{"%LineNum%", "id", "first_name", "group_name", "bio", "note (!!!)"},
			salesLines: 6,
		},
		{
			name: "horizontal_only_transformed", format: horizontalTableFormatName, onlyTransformed: true,
			columns: []string{"%LineNum%", "id", "first_name", "bio", "note (!!!)"},
		},
		{
			name: "horizontal_diff_only_transformed", format: horizontalTableFormatName, withDiff: true,
			onlyTransformed: true,
			columns:         []string{"%LineNum%", "id", "first_name", "bio", "note (!!!)"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tab, original, transformed := textDocumentTable(t)
			doc := NewTextDocument(tab, tt.withDiff, tt.onlyTransformed, tt.format)
			appendRows(t, doc, tab, original, transformed)

			var out bytes.Buffer
			require.NoError(t, doc.Print(&out))
			got := out.String()
			testutils.AssertGolden(t, "text_document_"+tt.name+".golden", testutils.StripANSI(got))
			testutils.AssertGolden(t, "text_document_"+tt.name+".colors.golden", testutils.NormalizeANSI(got))

			plain := testutils.StripANSI(got)
			assert.Contains(t, plain, "\"public\".\"people\"")
			cells := testutils.TableCells(got)
			require.NotEmpty(t, cells)

			// The record number is printed once per record in the merged first column.
			for lineIdx := range original {
				assert.Equalf(t, 1, testutils.CountCells(cells, 0, fmt.Sprintf("%d", lineIdx)), "record %d", lineIdx)
			}

			salesLines := 0
			for _, c := range cells {
				for _, v := range c {
					if v == "Sales" {
						salesLines++
						break
					}
				}
			}
			assert.Equal(t, tt.salesLines, salesLines)

			if tt.format == horizontalTableFormatName {
				header := cells[0]
				require.Len(t, header, len(tt.columns))
				for idx, name := range tt.columns {
					assert.Equal(t, name, header[idx])
				}
			} else {
				var names []string
				for _, c := range cells {
					name := strings.TrimSuffix(c[1], " (!!!)")
					if name != "" && name != "Column" {
						names = append(names, name)
					}
				}
				// Every record lists the columns in the same order.
				for idx := range original {
					assert.Equal(t, tt.columns, names[idx*len(tt.columns):(idx+1)*len(tt.columns)])
				}
			}

			// Short ASCII words survive wrapping as they are.
			words := []string{"Mark", "Anna", "NULL", "changed", "aliquip"}
			if tt.withDiff {
				words = append(words, "John", "keep", "Lorem", "labore")
			}
			for _, word := range words {
				assert.Contains(t, plain, word)
			}
		})
	}
}

func TestTextDocument_Print_horizontal_keeps_runs_of_spaces(t *testing.T) {
	tab, _, _ := textDocumentTable(t)
	// The run of spaces is at the 64th rune, where the long word cut used to break the line and drop it.
	bio := strings.Repeat("a", 20) + " " + strings.Repeat("b", 20) + " " + strings.Repeat("c", 20) + "   ddddd"
	original := [][]byte{[]byte("1\tJohn\tSales\t" + bio + "\t")}
	transformed := [][]byte{[]byte("1\tJohn\tSales\t" + strings.Replace(bio, "   ", " ", 1) + "\t")}
	doc := NewTextDocument(tab, true, false, horizontalTableFormatName)
	appendRows(t, doc, tab, original, transformed)

	var out bytes.Buffer
	require.NoError(t, doc.Print(&out))
	plain := testutils.StripANSI(out.String())
	assert.Contains(t, plain, strings.Repeat("c", 20)+"   ddddd")
	assert.Contains(t, plain, strings.Repeat("c", 20)+" ddddd")
}
