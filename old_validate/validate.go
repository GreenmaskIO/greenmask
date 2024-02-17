package old_validate

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/db/postgres/cmd/validate_utils"
	runtimeContext "github.com/greenmaskio/greenmask/internal/db/postgres/context"
	"github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/utils/reader"
	stringsUtils "github.com/greenmaskio/greenmask/internal/utils/strings"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const nullStringValue = "NULL"

const (
	horizontalTableFormatName = "horizontal"
	verticalTableFormatName   = "vertical"

	maxWrapLength = 64

	jsonFormat string = "json"
	textFormat string = "json"
)

type printSettings struct {
	OriginalColors    []tablewriter.Colors
	TransformedColors []tablewriter.Colors
	HeaderColors      []tablewriter.Colors
	ColumnsAlignments []int
}

type Validate struct {
	*cmd.Dump
	tmpDir string
}

func NewValidate(cfg *domains.Config, registry *utils.TransformerRegistry) (*Validate, error) {
	var st storages.Storager
	st, err := directory.NewStorage(&directory.Config{Path: cfg.Common.TempDirectory})
	if err != nil {
		return nil, fmt.Errorf("error initializing storage")
	}
	tmpDir := strconv.FormatInt(time.Now().UnixMilli(), 10)
	st = st.SubStorage(tmpDir, true)

	d := cmd.NewDump(cfg, st, registry)
	d.dumpIdSequence = toc.NewDumpSequence(0)
	d.validate = true
	return &Validate{
		Dump:   d,
		tmpDir: path.Join(cfg.Common.TempDirectory, tmpDir),
	}, nil
}

func (v *Validate) Run(ctx context.Context) error {

	defer func() {
		// Deleting temp dir after closing it
		if err := os.RemoveAll(v.tmpDir); err != nil {
			log.Warn().Err(err).Msgf("unable to delete temp directory")
		}
	}()
	if err := custom.BootstrapCustomTransformers(ctx, v.registry, v.config.CustomTransformers); err != nil {
		return fmt.Errorf("error bootstraping custom transformers: %w", err)
	}

	dsn, err := v.pgDumpOptions.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cannot build connection string: %w", err)
	}

	conn, err := v.connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	tx, err := v.startMainTx(ctx, conn)
	if err != nil {
		return fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	if err = v.gatherPgFacts(ctx, tx); err != nil {
		return fmt.Errorf("error gathering facts: %w", err)
	}

	// filter tables that must be validated if empty validate all
	var tablesToValidate []*domains.Table
	for _, tv := range v.config.Validate.Tables {
		var schemaName, tableName string
		parts := strings.Split(tv, ".")
		if len(parts) > 2 {
			return fmt.Errorf("wrong \"validate_table\" format \"%s\": value has %d coma symbols (.)", tv, len(parts))
		} else if len(parts) == 2 {
			schemaName = parts[0]
			tableName = parts[1]
		} else {
			tableName = parts[0]
		}

		var foundTable *domains.Table
		for _, t := range v.config.Dump.Transformation {
			if t.Schema == schemaName && t.Name == tableName {
				foundTable = t
				break
			}
			if schemaName == "" && t.Name == tableName {
				if foundTable != nil {
					return fmt.Errorf("wrong \"validate_table\" value: unable uniqually identify table \"%s\": sepcify schema name", tv)
				}
				foundTable = t
			}
		}
		if foundTable != nil {
			tablesToValidate = append(tablesToValidate, foundTable)
		} else {
			return fmt.Errorf("unable to find table from \"validate_table\" parameter: table %s is not found in transformation config", tv)
		}
	}

	if len(tablesToValidate) > 0 {
		v.config.Dump.Transformation = tablesToValidate
	}

	v.context, err = runtimeContext.NewRuntimeContext(ctx, tx, v.config.Dump.Transformation, v.registry,
		v.pgDumpOptions, v.version)
	if err != nil {
		return fmt.Errorf("unable to build runtime context: %w", err)
	}
	// TODO: Implement warnings hook, such as logging and HTTP sender
	for _, w := range v.context.Warnings {
		w.MakeHash()
		if idx := slices.Index(v.config.Validate.ResolvedWarnings, w.Hash); idx != -1 {
			log.Debug().Str("hash", w.Hash).Msg("resolved warning has been excluded")
			if w.Severity == toolkit.ErrorValidationSeverity {
				return fmt.Errorf("warning with hash %s cannot be excluded because it is an error", w.Hash)
			}
			continue
		}

		if w.Severity == toolkit.ErrorValidationSeverity {
			log.Error().Any("ValidationWarning", w).Msg("")
		} else {
			log.Warn().Any("ValidationWarning", w).Msg("")
		}
	}
	if v.context.IsFatal() {
		return fmt.Errorf("fatal validation error")
	}

	if !v.config.Validate.Data {
		return nil
	}

	var tablesWithTransformers []dump_objects.Entry
	for _, item := range v.context.DataSectionObjects {

		if t, ok := item.(*dump_objects.Table); ok && len(t.Transformers) > 0 {
			t.ValidateLimitedRecords = v.config.Validate.RowsLimit
			tablesWithTransformers = append(tablesWithTransformers, t)
		}
	}
	v.context.DataSectionObjects = tablesWithTransformers

	if err = v.dataDump(ctx); err != nil {
		return fmt.Errorf("data stage dumping error: %w", err)
	}

	for _, e := range v.dataEntries {
		idx := slices.IndexFunc(v.context.DataSectionObjects, func(entry dump_objects.Entry) bool {
			t := entry.(*dump_objects.Table)
			return t.DumpId == e.DumpId
		})

		t := v.context.DataSectionObjects[idx].(*dump_objects.Table)

		if err = v.printText(ctx, t); err != nil {
			return fmt.Errorf("error pretty printing table \"%s\".\"%s\": %w", t.Table.Schema, t.Table.Name, err)
		}
	}

	return nil
}

func (v *Validate) getVerticalRowColors(affectedColumns map[int]struct{}, columnIdx int, original, transformed *toolkit.RawValue) ([]tablewriter.Colors, bool) {
	var colors []tablewriter.Colors
	var isEqual bool
	if v.config.Validate.Diff {
		isEqual = validate_utils.ValuesEqual(original, transformed)
		colors = make([]tablewriter.Colors, 4)
	} else {
		colors = make([]tablewriter.Colors, 3)
	}
	colors[0] = tablewriter.Colors{}
	_, affected := affectedColumns[columnIdx]
	if affected || (v.config.Validate.Diff && !isEqual) {
		colors[1] = tablewriter.Colors{tablewriter.BgRedColor}
	} else {
		colors[1] = tablewriter.Colors{}
	}

	if v.config.Validate.Diff {
		if !isEqual {
			colors[2] = tablewriter.Colors{tablewriter.FgHiGreenColor}
			colors[3] = tablewriter.Colors{tablewriter.FgHiRedColor}
		} else {
			colors[2] = tablewriter.Colors{}
			colors[3] = tablewriter.Colors{}
		}
	} else {
		if affected {
			colors[2] = tablewriter.Colors{tablewriter.FgHiRedColor}
		} else {
			colors[2] = tablewriter.Colors{}
		}

	}
	return colors, isEqual
}

func (v *Validate) getAffectedColumns(t *dump_objects.Table) map[int]struct{} {
	affectedColumns := make(map[int]struct{})
	for _, tr := range t.Transformers {
		ac := tr.GetAffectedColumns()
		for idx := range ac {
			affectedColumns[idx] = struct{}{}
		}
	}
	return affectedColumns
}

func (v *Validate) getHorizontalSettings(t *dump_objects.Table) *printSettings {
	affectedColumns := v.getAffectedColumns(t)

	originalColumnsColors := make([]tablewriter.Colors, len(t.Columns))
	transformedColumnsColors := make([]tablewriter.Colors, len(t.Columns))
	headerColors := make([]tablewriter.Colors, len(t.Columns))
	columnsAlignments := make([]int, len(t.Columns))
	for idx := range t.Columns {
		if _, ok := affectedColumns[idx]; ok {
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

func (v *Validate) printHorizontally(ctx context.Context, t *dump_objects.Table) error {
	settings := v.getHorizontalSettings(t)

	prettyWriter := tablewriter.NewWriter(os.Stdout)
	prettyWriter.SetColumnAlignment(settings.ColumnsAlignments)

	row := *pgcopy.NewRow(len(t.Columns))
	tableData, err := v.st.GetObject(ctx, fmt.Sprintf("%d.dat.gz", t.DumpId))
	if err != nil {
		log.Err(err).Msg("")
	}
	defer tableData.Close()
	gz, err := gzip.NewReader(tableData)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}
	defer gz.Close()
	r := bufio.NewReader(gz)

	var lineNum = 1
	realAffectedColumns := v.getAffectedColumns(t)
	diffValues := make([][]*toolkit.RawValue, len(t.Columns))
	for idx := range t.Columns {
		diffValues[idx] = make([]*toolkit.RawValue, 2)
	}
	for {
		line, err := reader.ReadLine(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("unable to read line: %w", err)
		}

		// Handle end of dump_objects file seq
		if validate_utils.LineIsEndOfData(line) {
			break
		}

		if err = row.Decode(line); err != nil {
			return fmt.Errorf("error decoding copy line: %w", err)
		}
		record := make([]string, len(t.Columns))

		for idx, c := range t.Columns {
			value, err := row.GetColumn(idx)
			if err != nil {
				return fmt.Errorf("unable to get column \"%s\" value: %w", c.Name, err)
			}
			if value.IsNull {
				record[idx] = nullStringValue
			} else {
				record[idx] = stringsUtils.WrapString(string(value.Data), maxWrapLength)
			}
		}

		record = slices.Insert(record, 0, fmt.Sprintf("%d", lineNum))

		colors := settings.TransformedColors
		prettyWriter.Rich(record, colors)
		lineNum++
	}

	header := make([]string, len(t.Columns))
	for idx, c := range t.Columns {
		header[idx] = c.Name
	}
	header = slices.Insert(header, 0, "%LineNum%")
	headerColors := v.getVerticalHeaderColors(t, realAffectedColumns)

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

func (v *Validate) printHorizontallyWithDiff(ctx context.Context, t *dump_objects.Table) error {
	settings := v.getHorizontalSettings(t)

	prettyWriter := tablewriter.NewWriter(os.Stdout)
	prettyWriter.SetColumnAlignment(settings.ColumnsAlignments)

	tableData, err := v.st.GetObject(ctx, fmt.Sprintf("%d.dat.gz", t.DumpId))
	if err != nil {
		log.Err(err).Msg("")
	}
	defer tableData.Close()
	gz, err := gzip.NewReader(tableData)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}
	defer gz.Close()
	r := bufio.NewReader(gz)

	realAffectedColumns := v.getAffectedColumns(t)
	affectedColumns := v.getAffectedColumns(t)
	originalRow := pgcopy.NewRow(len(t.Columns))
	transformedRow := pgcopy.NewRow(len(t.Columns))
	var lineNum = 1

	for {

		var originalLine, transformedLine []byte
		var originalValue, transformedValue *toolkit.RawValue

		if v.config.Validate.Diff {
			originalLine, err = reader.ReadLine(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("unable to read line: %w", err)
			}
			// Handle end of dump_objects file seq
			if validate_utils.LineIsEndOfData(originalLine) {
				break
			}

			transformedLine, err = reader.ReadLine(r)
			if err != nil {
				return fmt.Errorf("unable to read line: %w", err)
			}

			if err = originalRow.Decode(originalLine); err != nil {
				return fmt.Errorf("error decoding copy line: %w", err)
			}
			if err = transformedRow.Decode(transformedLine); err != nil {
				return fmt.Errorf("error decoding copy line: %w", err)
			}
		} else {
			transformedLine, err = reader.ReadLine(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("unable to read line: %w", err)
			}
			// Handle end of dump_objects file seq
			if validate_utils.LineIsEndOfData(transformedLine) {
				break
			}
			if err = transformedRow.Decode(transformedLine); err != nil {
				return fmt.Errorf("error decoding copy line: %w", err)
			}
		}

		originalRecord := make([]string, len(t.Columns))
		transformedRecord := make([]string, len(t.Columns))
		originalRecordColors := make([]tablewriter.Colors, len(t.Columns))
		transformedRecordColors := make([]tablewriter.Colors, len(t.Columns))
		for idx := range t.Columns {
			originalValue, err = originalRow.GetColumn(idx)
			if err != nil {
				return err
			}
			if originalValue.IsNull {
				originalRecord[idx] = nullStringValue
			} else {
				originalRecord[idx] = stringsUtils.WrapString(string(originalValue.Data), maxWrapLength)
			}

			transformedValue, err = transformedRow.GetColumn(idx)
			if err != nil {
				return err
			}
			if transformedValue.IsNull {
				transformedRecord[idx] = nullStringValue
			} else {
				transformedRecord[idx] = stringsUtils.WrapString(string(transformedValue.Data), maxWrapLength)
			}

			if idx == 2 {
				log.Debug().Msg("")
			}
			if !validate_utils.ValuesEqual(originalValue, transformedValue) {
				originalRecordColors[idx] = tablewriter.Colors{tablewriter.FgHiGreenColor}
				transformedRecordColors[idx] = tablewriter.Colors{tablewriter.FgHiRedColor}
				realAffectedColumns[idx] = struct{}{}
			} else {
				originalRecordColors[idx] = []int{}
				transformedRecordColors[idx] = []int{}
			}
		}

		originalRecordColors = slices.Insert(originalRecordColors, 0, tablewriter.Colors{})
		transformedRecordColors = slices.Insert(transformedRecordColors, 0, tablewriter.Colors{})
		originalRecord = slices.Insert(originalRecord, 0, fmt.Sprintf("%d", lineNum))
		transformedRecord = slices.Insert(transformedRecord, 0, fmt.Sprintf("%d", lineNum))
		prettyWriter.Rich(originalRecord, originalRecordColors)
		prettyWriter.Rich(transformedRecord, transformedRecordColors)

		lineNum++
	}

	header := make([]string, len(t.Columns))
	for idx, c := range t.Columns {
		_, expected := affectedColumns[idx]
		_, notExpectedButChanged := realAffectedColumns[idx]
		if !expected && notExpectedButChanged {
			header[idx] = fmt.Sprintf("%s (!!!)", c.Name)
		} else {
			header[idx] = c.Name
		}

	}
	header = slices.Insert(header, 0, "%LineNum%")
	headerColors := v.getVerticalHeaderColors(t, realAffectedColumns)

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

func (v *Validate) getVerticalHeaderColors(t *dump_objects.Table, affectedColumns map[int]struct{}) []tablewriter.Colors {
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

func (v *Validate) printVertically(ctx context.Context, t *dump_objects.Table) error {

	var recordSize = 3
	if v.config.Validate.Diff {
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

	prettyWriter := tablewriter.NewWriter(os.Stdout)
	prettyWriter.SetAutoMergeCellsByColumnIndex([]int{0})
	prettyWriter.SetColumnAlignment(columnAlignments)
	prettyWriter.SetAutoWrapText(true)
	prettyWriter.SetHeaderLine(true)
	prettyWriter.SetRowLine(true)
	header := []string{"%LineNum%", "Column", "OriginalValue", "TransformedValue"}
	if !v.config.Validate.Diff {
		header = []string{"%LineNum%", "Column", "TransformedValue"}
	}

	affectedColumns := v.getAffectedColumns(t)

	originalRow := pgcopy.NewRow(len(t.Columns))
	transformedRow := pgcopy.NewRow(len(t.Columns))
	tableData, err := v.st.GetObject(ctx, fmt.Sprintf("%d.dat.gz", t.DumpId))
	if err != nil {
		log.Err(err).Msg("")
	}
	defer tableData.Close()
	gz, err := gzip.NewReader(tableData)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}
	defer gz.Close()
	r := bufio.NewReader(gz)

	var lineNum = 1
	for {
		var originalLine, transformedLine []byte
		var originalValue, transformedValue *toolkit.RawValue

		if v.config.Validate.Diff {
			originalLine, err = reader.ReadLine(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("unable to read line: %w", err)
			}
			// Handle end of dump_objects file seq
			if validate_utils.LineIsEndOfData(originalLine) {
				break
			}

			transformedLine, err = reader.ReadLine(r)
			if err != nil {
				return fmt.Errorf("unable to read line: %w", err)
			}

			if err = originalRow.Decode(originalLine); err != nil {
				return fmt.Errorf("error decoding copy line: %w", err)
			}
			if err = transformedRow.Decode(transformedLine); err != nil {
				return fmt.Errorf("error decoding copy line: %w", err)
			}
		} else {
			transformedLine, err = reader.ReadLine(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("unable to read line: %w", err)
			}
			// Handle end of dump_objects file seq
			if validate_utils.LineIsEndOfData(transformedLine) {
				break
			}
			if err = transformedRow.Decode(transformedLine); err != nil {
				return fmt.Errorf("error decoding copy line: %w", err)
			}
		}

		prettyWriter.Rich(header, headerColors)
		for idx, c := range t.Columns {
			record := make([]string, recordSize)
			record[0] = fmt.Sprintf("%d", lineNum)
			record[1] = c.Name

			if v.config.Validate.Diff {
				originalValue, err = originalRow.GetColumn(idx)
				if err != nil {
					return err
				}
				if originalValue.IsNull {
					record[2] = nullStringValue
				} else {
					record[2] = stringsUtils.WrapString(string(originalValue.Data), maxWrapLength)
				}

				transformedValue, err = transformedRow.GetColumn(idx)
				if err != nil {
					return err
				}
				if transformedValue.IsNull {
					record[3] = nullStringValue
				} else {
					record[3] = stringsUtils.WrapString(string(transformedValue.Data), maxWrapLength)
				}
			} else {
				transformedValue, err = transformedRow.GetColumn(idx)
				if err != nil {
					return err
				}
				if transformedValue.IsNull {
					record[2] = nullStringValue
				} else {
					record[2] = stringsUtils.WrapString(string(transformedValue.Data), maxWrapLength)
				}
			}

			colors, isEqual := v.getVerticalRowColors(affectedColumns, idx, originalValue, transformedValue)
			_, affected := affectedColumns[idx]
			if v.config.Validate.Diff && !isEqual && !affected {
				record[1] = fmt.Sprintf("%s (!!!)", c.Name)
			}
			prettyWriter.Rich(record, colors)
		}
		lineNum++
	}
	os.Stdout.Write([]byte(fmt.Sprintf("\n\n\t\"%s\".\"%s\"\n", t.Schema, t.Name)))
	prettyWriter.Render()

	return nil
}

func (v *Validate) print(ctx context.Context, t *dump_objects.Table, format string, withDiff bool) error {
	row := *pgcopy.NewRow(len(t.Columns))
	tableData, err := v.st.GetObject(ctx, fmt.Sprintf("%d.dat.gz", t.DumpId))
	if err != nil {
		log.Err(err).Msg("")
	}
	defer tableData.Close()
	gz, err := gzip.NewReader(tableData)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}
	defer gz.Close()
	r := bufio.NewReader(gz)

	var lineNum = 1
	realAffectedColumns := v.getAffectedColumns(t)
	diffValues := make([][]*toolkit.RawValue, len(t.Columns))
	for idx := range t.Columns {
		diffValues[idx] = make([]*toolkit.RawValue, 2)
	}
	for {
		line, err := reader.ReadLine(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("unable to read line: %w", err)
		}

		// Handle end of dump_objects file seq
		if validate_utils.LineIsEndOfData(line) {
			break
		}

		if err = row.Decode(line); err != nil {
			return fmt.Errorf("error decoding copy line: %w", err)
		}
		record := make([]string, len(t.Columns))
		for idx, c := range t.Columns {
			value, err := row.GetColumn(idx)
			if err != nil {
				return fmt.Errorf("unable to get column \"%s\" value: %w", c.Name, err)
			}
			if value.IsNull {
				record[idx] = nullStringValue
			} else {
				record[idx] = stringsUtils.WrapString(string(value.Data), maxWrapLength)
			}
		}

		record = slices.Insert(record, 0, fmt.Sprintf("%d", lineNum))

		lineNum++
	}

	header := make([]string, len(t.Columns))
	for idx, c := range t.Columns {
		header[idx] = c.Name
	}
	return nil
}

func (v *Validate) printText(ctx context.Context, t *dump_objects.Table) error {
	switch v.config.Validate.TableFormat {
	case horizontalTableFormatName:
		if v.config.Validate.Diff {
			return v.printHorizontallyWithDiff(ctx, t)
		} else {
			return v.printHorizontally(ctx, t)
		}
	case verticalTableFormatName:
		return v.printVertically(ctx, t)
	default:
		return fmt.Errorf("unknwon data format \"%s\"", v.config.Validate.TableFormat)
	}
}
