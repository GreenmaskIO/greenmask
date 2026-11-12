package validate_utils

import (
	"fmt"
	"io"
	"slices"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/utils/tableprinter"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	horizontalTableFormatName = "horizontal"
	verticalTableFormatName   = "vertical"
)

// maxWordLength is the length in runes at which the horizontal table cuts a long word.
const maxWordLength = 64

type TextDocument struct {
	*JsonDocument
	tableFormat string
}

func NewTextDocument(table *entries.Table, withDiff bool, onlyTransformed bool, tableFormat string) *TextDocument {
	jd := NewJsonDocument(table, withDiff, onlyTransformed)
	if tableFormat != horizontalTableFormatName && tableFormat != verticalTableFormatName {
		panic(fmt.Sprintf("unknown table format %s", tableFormat))
	}
	return &TextDocument{
		JsonDocument: jd,
		tableFormat:  tableFormat,
	}
}

func (td *TextDocument) Print(w io.Writer) error {
	switch td.tableFormat {
	case verticalTableFormatName:
		return td.printVertical(w)
	case horizontalTableFormatName:
		if td.withDiff {
			return td.printWithDiffHorizontal(w)
		}
		return td.printPlainHorizontal(w)
	}
	return nil
}

func (td *TextDocument) getVerticalRowColors(value *valueWithDiff) []text.Colors {
	colors := make([]text.Colors, 3)
	if td.withDiff {
		colors = make([]text.Colors, 4)
	}
	if !value.Equal {
		colors[1] = text.Colors{text.BgRed}
		colors[len(colors)-1] = text.Colors{text.FgHiRed}
		if td.withDiff {
			colors[2] = text.Colors{text.FgHiGreen}
		}
	}
	return colors
}

// newTable returns a validate table: headers printed as the column names are, cells aligned left and wrapped,
// and the first column, the record number, merged.
func newTable(header []string, headerColors []text.Colors, headerAlign text.Align) table.Writer {
	t := tableprinter.New()
	t.Style().Format.Header = text.FormatDefault
	t.Style().Options.SeparateRows = true
	configs := make([]table.ColumnConfig, len(header))
	for idx := range header {
		configs[idx] = table.ColumnConfig{
			Number:       idx + 1,
			Align:        text.AlignLeft,
			AlignHeader:  headerAlign,
			ColorsHeader: headerColors[idx],
		}
	}
	configs[0].AutoMerge = true
	t.SetColumnConfigs(configs)
	t.AppendHeader(tableprinter.Row(header))
	return t
}

// richRow wraps every cell and colors it. Colors are added after wrapping, so the table can measure the cells.
func richRow(record []string, colors []text.Colors) table.Row {
	row := make(table.Row, len(record))
	for idx, v := range record {
		row[idx] = tableprinter.Colorize(colors[idx], tableprinter.Wrap(v, tableprinter.WrapWidth))
	}
	return row
}

func (td *TextDocument) printVertical(w io.Writer) error {
	header := []string{"%LineNum%", "Column", "Value"}
	if td.withDiff {
		header = []string{"%LineNum%", "Column", "OriginalValue", "TransformedValue"}
	}
	headerColors := make([]text.Colors, len(header))
	for idx := range headerColors {
		headerColors[idx] = text.Colors{text.Bold}
	}
	prettyWriter := newTable(header, headerColors, text.AlignLeft)

	result := td.Get()
	colIdxsToPrint := td.getColumnsIdxsToPrint()

	for lineIdx, res := range result.RecordsWithDiff {
		for _, colIdx := range colIdxsToPrint {
			record := make([]string, len(header))
			record[0] = fmt.Sprintf("%d", lineIdx)
			colName := td.table.Columns[colIdx].Name
			colValue := res[colName]
			record[1] = colName
			if td.withDiff {
				record[2] = colValue.Original
				record[3] = colValue.Transformed
			} else {
				record[2] = colValue.Transformed
			}

			colors := td.getVerticalRowColors(colValue)
			if !colValue.Expected {
				record[1] = fmt.Sprintf("%s (!!!)", colName)
			}
			prettyWriter.AppendRow(richRow(record, colors))
		}
	}

	if err := td.writeTableTitle(w); err != nil {
		return err
	}
	return tableprinter.Render(w, prettyWriter)
}

func (td *TextDocument) getAffectedColumns() map[int]struct{} {
	res := make(map[int]struct{})
	colToPrint := td.GetAffectedColumns()
	for colName := range colToPrint {
		idx := slices.IndexFunc(td.table.Columns, func(column *toolkit.Column) bool {
			return column.Name == colName
		})
		if idx == -1 {
			panic("expected column to be found in the table column list")
		}
		res[idx] = struct{}{}
	}
	return res
}

func (td *TextDocument) getColumnsIdxsToPrint() []int {
	var res []int
	colToPrint := td.GetColumnsToPrint()
	for colName := range colToPrint {
		idx := slices.IndexFunc(td.table.Columns, func(column *toolkit.Column) bool {
			return column.Name == colName
		})
		if idx == -1 {
			panic("expected column to be found in the table column list")
		}
		res = append(res, idx)
	}
	slices.Sort(res)
	return res
}

// newHorizontalTable returns a horizontal table whose header has the line number and the columns to print.
func (td *TextDocument) newHorizontalTable(colIdxsToPrint []int) table.Writer {
	unexpectedlyChanged := td.GetUnexpectedlyChangedColumns()
	affectedColumns := td.getAffectedColumns()

	header := []string{"%LineNum%"}
	headerColors := []text.Colors{nil}
	for _, colIdx := range colIdxsToPrint {
		name := td.table.Columns[colIdx].Name
		if _, ok := unexpectedlyChanged[name]; ok {
			name = fmt.Sprintf("%s (!!!)", name)
		}
		header = append(header, name)

		var colors text.Colors
		if _, ok := affectedColumns[colIdx]; ok {
			colors = text.Colors{text.BgRed}
		}
		headerColors = append(headerColors, colors)
	}
	return newTable(header, headerColors, text.AlignCenter)
}

func (td *TextDocument) printWithDiffHorizontal(w io.Writer) error {
	result := td.Get()
	colIdxsToPrint := td.getColumnsIdxsToPrint()
	prettyWriter := td.newHorizontalTable(colIdxsToPrint)

	for lineIdx, res := range result.RecordsWithDiff {
		lineNum := fmt.Sprintf("%d", lineIdx)
		originalRecord := []string{lineNum}
		transformedRecord := []string{lineNum}
		originalRecordColors := []text.Colors{nil}
		transformedRecordColors := []text.Colors{nil}
		for _, colIdx := range colIdxsToPrint {
			colValue := res[td.table.Columns[colIdx].Name]
			originalRecord = append(originalRecord, tableprinter.CutLongWords(colValue.Original, maxWordLength))
			transformedRecord = append(transformedRecord, tableprinter.CutLongWords(colValue.Transformed, maxWordLength))

			var originalColors, transformedColors text.Colors
			if !colValue.Equal {
				originalColors = text.Colors{text.FgHiGreen}
				transformedColors = text.Colors{text.FgHiRed}
			}
			originalRecordColors = append(originalRecordColors, originalColors)
			transformedRecordColors = append(transformedRecordColors, transformedColors)
		}
		prettyWriter.AppendRow(richRow(originalRecord, originalRecordColors))
		prettyWriter.AppendRow(richRow(transformedRecord, transformedRecordColors))
	}

	if err := td.writeTableTitle(w); err != nil {
		return err
	}
	return tableprinter.Render(w, prettyWriter)
}

func (td *TextDocument) printPlainHorizontal(w io.Writer) error {
	result := td.Get()
	colIdxsToPrint := td.getColumnsIdxsToPrint()
	prettyWriter := td.newHorizontalTable(colIdxsToPrint)

	for lineIdx, res := range result.RecordsWithDiff {
		transformedRecord := []string{fmt.Sprintf("%d", lineIdx)}
		transformedRecordColors := []text.Colors{nil}
		for _, colIdx := range colIdxsToPrint {
			colValue := res[td.table.Columns[colIdx].Name]
			transformedRecord = append(transformedRecord, tableprinter.CutLongWords(colValue.Transformed, maxWordLength))

			var transformedColors text.Colors
			if !colValue.Equal {
				transformedColors = text.Colors{text.FgHiRed}
			}
			transformedRecordColors = append(transformedRecordColors, transformedColors)
		}
		prettyWriter.AppendRow(richRow(transformedRecord, transformedRecordColors))
	}

	if err := td.writeTableTitle(w); err != nil {
		return err
	}
	return tableprinter.Render(w, prettyWriter)
}

func (td *TextDocument) writeTableTitle(w io.Writer) error {
	_, err := fmt.Fprintf(w, "\n\n\t\"%s\".\"%s\"\n", td.table.Schema, td.table.Name)
	if err != nil {
		return fmt.Errorf("error writing title: %w", err)
	}
	return nil
}
