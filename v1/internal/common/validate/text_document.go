package validate

import (
	"bytes"
	"fmt"
	"io"
	"slices"

	"github.com/olekukonko/tablewriter"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type TableFormat string

const (
	TableFormatNameHorizontal TableFormat = "horizontal"
	TableFormatNameVertical   TableFormat = "vertical"
)

func (m TableFormat) Validate() error {
	switch m {
	case TableFormatNameHorizontal, TableFormatNameVertical:
		return nil
	}
	return fmt.Errorf("table format '%s': %w", m, commonmodels.ErrValueValidationFailed)
}

const maxWrapLength = 64

type printSettings struct {
	OriginalColors    []tablewriter.Colors
	TransformedColors []tablewriter.Colors
	HeaderColors      []tablewriter.Colors
	ColumnsAlignments []int
}

type TextDocument struct {
	*JsonDocument
	tableFormat TableFormat
}

func NewTextDocument(
	table commonmodels.Table,
	affectedColumns []int,
	withDiff bool,
	onlyTransformed bool,
	tableFormat TableFormat,
) *TextDocument {
	jd := NewJsonDocument(table, affectedColumns, withDiff, onlyTransformed)
	if err := tableFormat.Validate(); err != nil {
		panic(fmt.Sprintf("invalid table format: %v", err))
	}
	return &TextDocument{
		JsonDocument: jd,
		tableFormat:  tableFormat,
	}
}

func (td *TextDocument) Marshall() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	switch td.tableFormat {
	case TableFormatNameVertical:
		if err := td.printVertical(buf); err != nil {
			return nil, err
		}
	case TableFormatNameHorizontal:
		if td.withDiff {
			if err := td.printWithDiffHorizontal(buf); err != nil {
				return nil, err
			}
		} else {
			if err := td.printPlainHorizontal(buf); err != nil {
				return nil, err
			}
		}

	default:
		panic(fmt.Sprintf("invalid table format: %v", td.tableFormat))
	}
	res, err := io.ReadAll(buf)
	if err != nil {
		return nil, fmt.Errorf("read buffer: %w", err)
	}
	return res, nil
}

func (td *TextDocument) getVerticalRowColors(value valueWithDiff) []tablewriter.Colors {
	var colors []tablewriter.Colors

	colors = make([]tablewriter.Colors, 4)
	if !td.withDiff {
		colors = make([]tablewriter.Colors, 3)
	}
	colors[0] = tablewriter.Colors{}

	colors[1] = tablewriter.Colors{}
	if !value.Equal {
		colors[1] = tablewriter.Colors{tablewriter.BgRedColor}
	}

	if td.withDiff {
		colors[2] = tablewriter.Colors{}
		colors[3] = tablewriter.Colors{}
		if !value.Equal {
			colors[2] = tablewriter.Colors{tablewriter.FgHiGreenColor}
			colors[3] = tablewriter.Colors{tablewriter.FgHiRedColor}
		}
	} else {
		colors[2] = tablewriter.Colors{}
		if !value.Equal {
			colors[2] = tablewriter.Colors{tablewriter.FgHiRedColor}
		}
	}
	return colors
}

func (td *TextDocument) printVertical(w io.Writer) error {

	recordSize := 3
	if td.withDiff {
		recordSize = 4
	}

	headerColorSetting := []int{tablewriter.Bold}
	alignmentSettings := tablewriter.ALIGN_LEFT
	headerColors := make([]tablewriter.Colors, recordSize)
	columnAlignments := make([]int, recordSize)
	for idx := range headerColors {
		headerColors[idx] = headerColorSetting
		columnAlignments[idx] = alignmentSettings
	}

	prettyWriter := tablewriter.NewWriter(w)
	prettyWriter.SetAutoMergeCellsByColumnIndex([]int{0})
	prettyWriter.SetColumnAlignment(columnAlignments)
	prettyWriter.SetAutoWrapText(true)
	prettyWriter.SetHeaderLine(true)
	prettyWriter.SetRowLine(true)

	header := []string{"%LineNum%", "Column", "Value"}
	if td.withDiff {
		header = []string{"%LineNum%", "Column", "OriginalValue", "TransformedValue"}
	}
	prettyWriter.Rich(header, headerColors)

	result := td.JsonDocument.Get()
	colIdxsToPrint := td.getColumnsIdxsToPrint()

	for lineIdx, res := range result.RecordsWithDiff {
		for _, colIdx := range colIdxsToPrint {
			record := make([]string, recordSize)
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
			prettyWriter.Rich(record, colors)
		}
	}

	if err := td.writeTableTitle(w); err != nil {
		return err
	}
	prettyWriter.Render()

	return nil
}

func (td *TextDocument) getAffectedColumns() map[int]struct{} {
	res := make(map[int]struct{})
	colToPrint := td.GetAffectedColumns()
	for colIdx := range colToPrint {
		idx := slices.IndexFunc(td.table.Columns, func(column commonmodels.Column) bool {
			return column.Idx == colIdx
		})
		if idx == -1 {
			panic("expected column to be found in the table column list")
		}
		res[colIdx] = struct{}{}
	}
	return res
}

func (td *TextDocument) getColumnsIdxsToPrint() []int {
	var res []int
	colToPrint := td.GetColumnsToPrint()
	for colIdx := range colToPrint {
		idx := slices.IndexFunc(td.table.Columns, func(column commonmodels.Column) bool {
			return column.Idx == colIdx
		})
		if idx == -1 {
			panic("expected column to be found in the table column list")
		}
		res = append(res, colIdx)
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
			originalRecord[tableColIdx] = WrapString(colValue.Original, maxWrapLength)
			transformedRecord[tableColIdx] = WrapString(colValue.Transformed, maxWrapLength)

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
		if _, ok := unexpectedlyChanged[c.Idx]; ok {
			header[tableColIdx] = fmt.Sprintf("%s (!!!)", c.Name)
		}
	}
	header = slices.Insert(header, 0, "%LineNum%")
	headerColors := td.getVerticalHorizontalColors()

	if err := td.writeTableTitle(w); err != nil {
		return err
	}
	prettyWriter.SetHeader(header)
	prettyWriter.SetRowLine(true)
	prettyWriter.SetAutoMergeCellsByColumnIndex([]int{0})
	prettyWriter.SetAutoWrapText(true)
	prettyWriter.SetHeaderLine(true)
	prettyWriter.SetHeaderColor(headerColors...)

	prettyWriter.Render()
	return nil
}

func (td *TextDocument) writeTableTitle(w io.Writer) error {
	_, err := w.Write([]byte(fmt.Sprintf("\n\n\t\"%s\".\"%s\"\n", td.table.Schema, td.table.Name)))
	if err != nil {
		return fmt.Errorf("error writing title: %w", err)
	}
	return nil
}

func (td *TextDocument) printPlainHorizontal(w io.Writer) error {
	settings := td.getHorizontalSettings()
	prettyWriter := tablewriter.NewWriter(w)
	prettyWriter.SetColumnAlignment(settings.ColumnsAlignments)

	result := td.JsonDocument.Get()
	colIdxsToPrint := td.getColumnsIdxsToPrint()

	for lineIdx, res := range result.RecordsWithDiff {
		transformedRecord := make([]string, len(colIdxsToPrint))
		transformedRecordColors := make([]tablewriter.Colors, len(colIdxsToPrint))
		for tableColIdx, colIdx := range colIdxsToPrint {
			colName := td.table.Columns[colIdx].Name
			colValue := res[colName]
			transformedRecord[tableColIdx] = WrapString(colValue.Transformed, maxWrapLength)

			transformedRecordColors[tableColIdx] = []int{}
			if !colValue.Equal {
				transformedRecordColors[tableColIdx] = tablewriter.Colors{tablewriter.FgHiRedColor}
			}
		}

		// Adding Line number columns
		transformedRecordColors = slices.Insert(transformedRecordColors, 0, tablewriter.Colors{})
		transformedRecord = slices.Insert(transformedRecord, 0, fmt.Sprintf("%d", lineIdx))

		prettyWriter.Rich(transformedRecord, transformedRecordColors)
	}

	unexpectedlyChanged := td.GetUnexpectedlyChangedColumns()
	header := make([]string, len(colIdxsToPrint))
	for tableColIdx, colIdx := range colIdxsToPrint {
		c := td.table.Columns[colIdx]
		header[tableColIdx] = c.Name
		if _, ok := unexpectedlyChanged[c.Idx]; ok {
			header[tableColIdx] = fmt.Sprintf("%s (!!!)", c.Name)
		}
	}
	header = slices.Insert(header, 0, "%LineNum%")
	headerColors := td.getVerticalHorizontalColors()

	if err := td.writeTableTitle(w); err != nil {
		return err
	}
	prettyWriter.SetHeader(header)
	prettyWriter.SetRowLine(true)
	prettyWriter.SetAutoMergeCellsByColumnIndex([]int{0})
	prettyWriter.SetAutoWrapText(true)
	prettyWriter.SetHeaderLine(true)
	prettyWriter.SetHeaderColor(headerColors...)

	prettyWriter.Render()
	return nil
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
