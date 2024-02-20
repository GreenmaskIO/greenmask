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
	tableFormat string
}

func NewTextDocument(table *dump_objects.Table, withDiff bool, onlyTransformed bool, tableFormat string) *TextDocument {
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
		if td.withDiff {
			return td.printWithDiffVertical(w)
		}
		return td.printPlainVertical(w)
	case horizontalTableFormatName:
		if td.withDiff {
			return td.printWithDiffHorizontal(w)
		}
		return td.printPlainHorizontal(w)
	}
	return nil
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

func (td *TextDocument) getVerticalHorizontalColors() []tablewriter.Colors {
	columnsToPrint := td.getColumnsIdxsToPrint()
	affectedColumns := td.getAffectedColumns()

	headerColors := make([]tablewriter.Colors, len(columnsToPrint))
	for tableColIdx, colIdx := range columnsToPrint {
		if _, ok := affectedColumns[colIdx]; ok {
			headerColors[tableColIdx] = []int{tablewriter.BgRedColor}
		} else {
			headerColors[tableColIdx] = []int{}
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
		originalRecord := make([]string, len(colIdxsToPrint))
		transformedRecord := make([]string, len(colIdxsToPrint))
		originalRecordColors := make([]tablewriter.Colors, len(colIdxsToPrint))
		transformedRecordColors := make([]tablewriter.Colors, len(colIdxsToPrint))
		for tableColIdx, colIdx := range colIdxsToPrint {
			colName := td.table.Columns[colIdx].Name
			colValue := res[colName]
			originalRecord[tableColIdx] = stringsUtils.WrapString(colValue.Original, maxWrapLength)
			transformedRecord[tableColIdx] = stringsUtils.WrapString(colValue.Transformed, maxWrapLength)

			originalRecordColors[tableColIdx] = []int{}
			transformedRecordColors[tableColIdx] = []int{}
			if !colValue.Equal {
				originalRecordColors[tableColIdx] = tablewriter.Colors{tablewriter.FgHiGreenColor}
				transformedRecordColors[tableColIdx] = tablewriter.Colors{tablewriter.FgHiRedColor}
			}
		}

		// Adding Line number columns
		originalRecordColors = slices.Insert(originalRecordColors, 0, tablewriter.Colors{})
		transformedRecordColors = slices.Insert(transformedRecordColors, 0, tablewriter.Colors{})
		originalRecord = slices.Insert(originalRecord, 0, fmt.Sprintf("%d", lineIdx))
		transformedRecord = slices.Insert(transformedRecord, 0, fmt.Sprintf("%d", lineIdx))

		prettyWriter.Rich(originalRecord, originalRecordColors)
		prettyWriter.Rich(transformedRecord, transformedRecordColors)
	}

	unexpectedlyChanged := td.GetUnexpectedlyChangedColumns()
	header := make([]string, len(colIdxsToPrint))
	for tableColIdx, colIdx := range colIdxsToPrint {
		c := td.table.Columns[colIdx]
		header[tableColIdx] = c.Name
		if _, ok := unexpectedlyChanged[c.Name]; ok {
			header[tableColIdx] = fmt.Sprintf("%s (!!!)", c.Name)
		}
	}
	header = slices.Insert(header, 0, "%LineNum%")
	headerColors := td.getVerticalHorizontalColors()

	os.Stdout.Write([]byte(fmt.Sprintf("\n\n\t\"%s\".\"%s\"\n", td.table.Schema, td.table.Name)))
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
	columnsToPrint := td.getColumnsIdxsToPrint()
	affectedColumns := td.getAffectedColumns()

	originalColumnsColors := make([]tablewriter.Colors, len(columnsToPrint))
	transformedColumnsColors := make([]tablewriter.Colors, len(columnsToPrint))
	headerColors := make([]tablewriter.Colors, len(columnsToPrint))
	columnsAlignments := make([]int, len(columnsToPrint))
	for tableColIdx, colIdx := range columnsToPrint {
		if _, ok := affectedColumns[colIdx]; ok {
			originalColumnsColors[tableColIdx] = []int{tablewriter.FgHiGreenColor}
			transformedColumnsColors[tableColIdx] = []int{tablewriter.FgHiRedColor}
			headerColors[tableColIdx] = []int{tablewriter.BgRedColor}
		} else {
			originalColumnsColors[tableColIdx] = []int{}
			transformedColumnsColors[tableColIdx] = []int{}
			headerColors[tableColIdx] = []int{}
		}
		columnsAlignments[tableColIdx] = tablewriter.ALIGN_LEFT
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
