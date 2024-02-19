package validate_utils

import (
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/olekukonko/tablewriter"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	stringsUtils "github.com/greenmaskio/greenmask/internal/utils/strings"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	horizontalTableFormatName = "horizontal"
	verticalTableFormatName   = "vertical"
)

const maxWrapLength = 64

type printSettings struct {
	OriginalColors    []tablewriter.Colors
	TransformedColors []tablewriter.Colors
	HeaderColors      []tablewriter.Colors
	ColumnsAlignments []int
}

type TextDocument struct {
	*JsonDocument
}

func (td *TextDocument) Print(w io.Writer) error {
	panic("implement me")
}

func (td *TextDocument) printWithDiffVertical(w io.Writer) error {
	panic("implement me")
}

func (td *TextDocument) getColumnsIdxsUnexpected() []int {
	var res []int
	colToPrint := td.GetUnexpectedlyChangedColumns()
	for colName := range colToPrint {
		idx := slices.IndexFunc(td.table.Columns, func(column *toolkit.Column) bool {
			return column.Name == colName
		})
		if idx != -1 {
			panic("expected column to be found in the table column list")
		}
		res = append(res, idx)
	}
	slices.Sort(res)
	return res
}

func (td *TextDocument) getColumnsIdxsToPrint() []int {
	var res []int
	colToPrint := td.GetColumnsToPrint()
	for colName := range colToPrint {
		idx := slices.IndexFunc(td.table.Columns, func(column *toolkit.Column) bool {
			return column.Name == colName
		})
		if idx != -1 {
			panic("expected column to be found in the table column list")
		}
		res = append(res, idx)
	}
	slices.Sort(res)
	return res
}

func (td *TextDocument) getVerticalHorizontalColors(t *dump_objects.Table, affectedColumns map[int]struct{}) []tablewriter.Colors {
	headerColors := make([]tablewriter.Colors, len(t.Columns))
	for idx := range t.Columns {
		if _, ok := affectedColumns[idx]; ok {
			headerColors[idx] = []int{tablewriter.BgRedColor}
		} else {
			headerColors[idx] = []int{}
		}
	}
	// Adding formatting setting for LineNum
	headerColors = slices.Insert(headerColors, 0, tablewriter.Colors{})
	return headerColors
}

func (td *TextDocument) printWithDiffHorizontal(w io.Writer) error {
	settings := td.getHorizontalSettings()
	prettyWriter := tablewriter.NewWriter(w)
	prettyWriter.SetColumnAlignment(settings.ColumnsAlignments)

	result := td.JsonDocument.Get()
	colIdxsToPrint := td.getColumnsIdxsToPrint()

	for lineIdx, res := range result.RecordsWithDiff {
		originalRecord := make([]string, len(td.table.Columns))
		transformedRecord := make([]string, len(td.table.Columns))
		originalRecordColors := make([]tablewriter.Colors, len(td.table.Columns))
		transformedRecordColors := make([]tablewriter.Colors, len(td.table.Columns))
		for _, colIdx := range colIdxsToPrint {
			colName := td.table.Columns[colIdx].Name
			colValue := res[colName]
			originalRecord[colIdx] = stringsUtils.WrapString(colValue.Original, maxWrapLength)
			transformedRecord[colIdx] = stringsUtils.WrapString(colValue.Transformed, maxWrapLength)

			if !colValue.Equal {
				originalRecordColors[colIdx] = tablewriter.Colors{tablewriter.FgHiGreenColor}
				transformedRecordColors[colIdx] = tablewriter.Colors{tablewriter.FgHiRedColor}
			} else {
				originalRecordColors[colIdx] = []int{}
				transformedRecordColors[colIdx] = []int{}
			}

			originalRecordColors = slices.Insert(originalRecordColors, 0, tablewriter.Colors{})
			transformedRecordColors = slices.Insert(transformedRecordColors, 0, tablewriter.Colors{})
			originalRecord = slices.Insert(originalRecord, 0, fmt.Sprintf("%d", lineIdx))
			transformedRecord = slices.Insert(transformedRecord, 0, fmt.Sprintf("%d", lineIdx))
			prettyWriter.Rich(originalRecord, originalRecordColors)
			prettyWriter.Rich(transformedRecord, transformedRecordColors)
		}
	}

	unexpectedlyChanged := td.GetUnexpectedlyChangedColumns()
	header := make([]string, len(colIdxsToPrint))
	for tableColIdx, colIdx := range colIdxsToPrint {
		c := td.table.Columns[colIdx]
		if _, ok := unexpectedlyChanged[c.Name]; ok {
			header[tableColIdx] = fmt.Sprintf("%s (!!!)", c.Name)
		} else {
			header[tableColIdx] = c.Name
		}
	}
	header = slices.Insert(header, 0, "%LineNum%")
	headerColors := td.getVerticalHorizontalColors(t, realAffectedColumns)

	os.Stdout.Write([]byte(fmt.Sprintf("\n\n\t\"%s\".\"%s\"\n", t.Schema, t.Name)))
	prettyWriter.SetHeader(header)
	prettyWriter.SetRowLine(true)
	prettyWriter.SetAutoMergeCellsByColumnIndex([]int{0})
	prettyWriter.SetAutoWrapText(true)
	prettyWriter.SetHeaderLine(true)
	prettyWriter.SetHeaderColor(headerColors...)

	prettyWriter.Render()
	return nil
}

func (td *TextDocument) printPlainHorizontal(w io.Writer) error {
	panic("implement me")
}

func (td *TextDocument) printPlainVertical(w io.Writer) error {
	panic("implement me")
}

func (td *TextDocument) getHorizontalSettings() *printSettings {
	affectedColumns := td.JsonDocument.GetColumnsToPrint()

	originalColumnsColors := make([]tablewriter.Colors, len(td.JsonDocument.table.Columns))
	transformedColumnsColors := make([]tablewriter.Colors, len(td.JsonDocument.table.Columns))
	headerColors := make([]tablewriter.Colors, len(td.JsonDocument.table.Columns))
	columnsAlignments := make([]int, len(td.JsonDocument.table.Columns))
	for idx, c := range td.JsonDocument.table.Columns {
		if _, ok := affectedColumns[c.Name]; ok {
			originalColumnsColors[idx] = []int{tablewriter.FgHiGreenColor}
			transformedColumnsColors[idx] = []int{tablewriter.FgHiRedColor}
			headerColors[idx] = []int{tablewriter.BgRedColor}
		} else {
			originalColumnsColors[idx] = []int{}
			headerColors[idx] = []int{}
			transformedColumnsColors[idx] = []int{}
		}
		columnsAlignments[idx] = tablewriter.ALIGN_LEFT
	}
	// Adding formatting setting for LineNum
	originalColumnsColors = slices.Insert(originalColumnsColors, 0, tablewriter.Colors{})
	headerColors = slices.Insert(headerColors, 0, tablewriter.Colors{})
	transformedColumnsColors = slices.Insert(transformedColumnsColors, 0, tablewriter.Colors{})
	columnsAlignments = slices.Insert(columnsAlignments, 0, tablewriter.ALIGN_LEFT)

	return &printSettings{
		OriginalColors:    originalColumnsColors,
		TransformedColors: transformedColumnsColors,
		HeaderColors:      headerColors,
		ColumnsAlignments: columnsAlignments,
	}
}
