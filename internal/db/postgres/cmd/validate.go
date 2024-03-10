package cmd

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/cmd/validate_utils"
	runtimeContext "github.com/greenmaskio/greenmask/internal/db/postgres/context"
	"github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	storageDto "github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/reader"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	JsonFormat string = "json"
	TextFormat string = "text"
)

const (
	VerticalTableFormat   = "vertical"
	HorizontalTableFormat = "horizontal"
)

const (
	nonZeroExitCode = 1
	zeroExitCode    = 0
)

type closeFunc func()

type Validate struct {
	*Dump
	tmpDir   string
	mainSt   storages.Storager
	exitCode int
}

func NewValidate(cfg *domains.Config, registry *utils.TransformerRegistry, st storages.Storager) (*Validate, error) {
	mainSt := st
	tmpDirName := strconv.FormatInt(time.Now().UnixMilli(), 10)
	st = st.SubStorage(tmpDirName, true)

	d := NewDump(cfg, st, registry)
	d.dumpIdSequence = toc.NewDumpSequence(0)

	d.validate = true
	return &Validate{
		Dump:     d,
		tmpDir:   tmpDirName,
		exitCode: zeroExitCode,
		mainSt:   mainSt,
	}, nil
}

func (v *Validate) Run(ctx context.Context) (int, error) {

	defer func() {
		if !v.config.Validate.Diff {
			return
		}
		if err := v.mainSt.Delete(ctx, v.tmpDir); err != nil {
			log.Warn().Err(err).Msg("error deleting temporary directory")
		}
	}()
	if err := custom.BootstrapCustomTransformers(ctx, v.registry, v.config.CustomTransformers); err != nil {
		return nonZeroExitCode, fmt.Errorf("error bootstraping custom transformers: %w", err)
	}

	dsn, err := v.pgDumpOptions.GetPgDSN()
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("cannot build connection string: %w", err)
	}

	conn, err := v.connect(ctx, dsn)
	if err != nil {
		return nonZeroExitCode, err
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	tx, err := v.startMainTx(ctx, conn)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	if err = v.gatherPgFacts(ctx, tx); err != nil {
		return nonZeroExitCode, fmt.Errorf("error gathering facts: %w", err)
	}

	// Get list of tables to validate
	tablesToValidate, err := v.getTablesToValidate()
	if err != nil {
		return nonZeroExitCode, err
	}
	v.config.Dump.Transformation = tablesToValidate

	v.context, err = runtimeContext.NewRuntimeContext(ctx, tx, v.config.Dump.Transformation, v.registry,
		v.pgDumpOptions, v.version)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("unable to build runtime context: %w", err)
	}

	if err = v.printValidationWarnings(); err != nil {
		return nonZeroExitCode, err
	}

	if err = v.diffWithPreviousSchema(ctx); err != nil {
		return nonZeroExitCode, err
	}

	if !v.config.Validate.Data {
		return v.exitCode, nil
	}

	if err = v.dumpTables(ctx); err != nil {
		return nonZeroExitCode, err
	}

	if err = v.print(ctx); err != nil {
		return nonZeroExitCode, err
	}

	return v.exitCode, nil
}

func (v *Validate) print(ctx context.Context) error {
	for _, e := range v.dataEntries {
		idx := slices.IndexFunc(v.context.DataSectionObjects, func(entry dump_objects.Entry) bool {
			t := entry.(*dump_objects.Table)
			return t.DumpId == e.DumpId
		})

		t := v.context.DataSectionObjects[idx].(*dump_objects.Table)
		doc, err := v.createDocument(ctx, t)
		if err != nil {
			return fmt.Errorf("unable to create validation document: %w", err)
		}

		if err = doc.Print(os.Stdout); err != nil {
			return fmt.Errorf("unable to print validation document: %w", err)
		}
	}
	return nil
}

func (v *Validate) getDocument(table *dump_objects.Table) validate_utils.Documenter {
	switch v.config.Validate.Format {
	case JsonFormat:
		return validate_utils.NewJsonDocument(table, v.config.Validate.Diff, v.config.Validate.OnlyTransformed)
	case TextFormat:
		return validate_utils.NewTextDocument(
			table, v.config.Validate.Diff, v.config.Validate.OnlyTransformed, v.config.Validate.TableFormat,
		)
	default:
		panic(fmt.Sprintf("unknown format %s", v.config.Validate.Format))
	}
}

func (v *Validate) getReader(ctx context.Context, table *dump_objects.Table) (closeFunc, *bufio.Reader, error) {
	tableData, err := v.st.GetObject(ctx, fmt.Sprintf("%d.dat.gz", table.DumpId))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get object from storage: %w", err)
	}

	gz, err := gzip.NewReader(tableData)
	if err != nil {
		tableData.Close()
		return nil, nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}

	f := func() {
		if err := tableData.Close(); err != nil {
			log.Warn().Err(err).Msg("caused error when closing reader object")
		}
		if err := gz.Close(); err != nil {
			log.Warn().Err(err).Msg("caused error when closing gzip reader")
		}
	}

	return f, bufio.NewReader(gz), nil
}

func (v *Validate) readRecords(r *bufio.Reader, t *dump_objects.Table) (original, transformed *pgcopy.Row, err error) {
	var originalLine, transformedLine []byte
	var originalRow, transformedRow *pgcopy.Row

	originalRow = pgcopy.NewRow(len(t.Columns))
	transformedRow = pgcopy.NewRow(len(t.Columns))

	originalLine, err = reader.ReadLine(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("unable to read line: %w", err)
	}
	// Handle end of dump_objects file seq
	if validate_utils.LineIsEndOfData(originalLine) {
		return nil, nil, io.EOF
	}

	transformedLine, err = reader.ReadLine(r)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read line: %w", err)
	}

	if err = originalRow.Decode(originalLine); err != nil {
		return nil, nil, fmt.Errorf("error decoding copy line: %w", err)
	}
	if err = transformedRow.Decode(transformedLine); err != nil {
		return nil, nil, fmt.Errorf("error decoding copy line: %w", err)
	}
	return originalRow, transformedRow, nil
}

func (v *Validate) createDocument(ctx context.Context, t *dump_objects.Table) (validate_utils.Documenter, error) {
	doc := v.getDocument(t)

	closeReader, r, err := v.getReader(ctx, t)
	if err != nil {
		return nil, err
	}
	defer closeReader()

	var line int
	for {

		original, transformed, err := v.readRecords(r, t)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if err := doc.Append(original, transformed); err != nil {
			return nil, fmt.Errorf("unable to append line %d to document: %w", line, err)
		}

		line++
	}

	return doc, nil
}

func (v *Validate) dumpTables(ctx context.Context) error {
	var tablesWithTransformers []dump_objects.Entry
	for _, item := range v.context.DataSectionObjects {

		if t, ok := item.(*dump_objects.Table); ok && len(t.Transformers) > 0 {
			t.ValidateLimitedRecords = v.config.Validate.RowsLimit
			tablesWithTransformers = append(tablesWithTransformers, t)
		}
	}
	v.context.DataSectionObjects = tablesWithTransformers

	if err := v.dataDump(ctx); err != nil {
		return fmt.Errorf("data stage dumping error: %w", err)
	}
	return nil
}

func (v *Validate) printValidationWarnings() error {
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
			// The warnings with error severity must be printed anyway
			log.Error().Any("ValidationWarning", w).Msg("")
		} else {
			// Print warnings with severity level lower than ErrorValidationSeverity only if requested
			if v.config.Validate.Warnings {
				log.Warn().Any("ValidationWarning", w).Msg("")
			}
		}
	}
	if v.context.IsFatal() {
		return fmt.Errorf("fatal validation error")
	}
	return nil
}

func (v *Validate) getTablesToValidate() ([]*domains.Table, error) {
	var tablesToValidate []*domains.Table
	for _, tv := range v.config.Validate.Tables {

		schemaName, tableName, err := parseTableName(tv)
		if err != nil {
			return nil, err
		}

		foundTable, err := findTableBySchemaAndName(v.config.Dump.Transformation, schemaName, tableName)
		if err != nil {
			return nil, err
		}

		if foundTable != nil {
			tablesToValidate = append(tablesToValidate, foundTable)
		}
	}

	if len(tablesToValidate) == 0 {
		return v.config.Dump.Transformation, nil
	}

	return tablesToValidate, nil
}

func (v *Validate) diffWithPreviousSchema(ctx context.Context) error {
	if !v.config.Validate.Schema {
		return nil
	}

	dumpId, err := v.getPreviousDumpId(ctx)
	if err != nil {
		return fmt.Errorf("cannot get previous dump id: %w", err)
	}
	if dumpId == "" {
		return nil
	}

	md, err := v.getPreviousMetadata(ctx, dumpId)
	if err != nil {
		return fmt.Errorf("cannot get previous metadata: %w", err)
	}

	diff := md.DatabaseSchema.Diff(v.context.DatabaseSchema)
	if len(diff) > 0 {
		v.exitCode = nonZeroExitCode

		err = v.printSchemaDiff(diff, dumpId)
		if err != nil {
			return fmt.Errorf("cannot print schema diff: %w", err)
		}
	}

	return nil
}

func (v *Validate) printSchemaDiff(diff []*toolkit.DiffNode, previousDumpId string) error {

	if v.config.Validate.Format == JsonFormat {
		data, err := json.Marshal(diff)
		if err != nil {
			return fmt.Errorf("cannot encode diff node: %w", err)
		}
		log.Warn().
			Str("PreviousDumpId", previousDumpId).
			RawJSON("Diff", data).
			Str("Hint", "Check schema changes before making new dump").
			Msg("Database schema has been changed")
		return nil
	}
	log.Warn().
		Str("PreviousDumpId", previousDumpId).
		Str("Hint", "Check schema changes before making new dump").
		Msg("Database schema has been changed")
	for _, node := range diff {
		log.Warn().Str("Event", node.Event).Any("Signature", node.Signature).Msg("")
	}

	return nil
}

func (v *Validate) getPreviousDumpId(ctx context.Context) (string, error) {
	var backupNames []string

	_, dirs, err := v.mainSt.ListDir(ctx)
	if err != nil {
		return "", fmt.Errorf("cannot walk through directory: %w", err)
	}
	for _, dir := range dirs {
		exists, err := dir.Exists(ctx, "metadata.json")
		if err != nil {
			return "", fmt.Errorf("cannot check file existence: %w", err)
		}
		if exists {
			backupNames = append(backupNames, dir.Dirname())
		}
	}

	slices.SortFunc(
		backupNames, func(a, b string) int {
			if a > b {
				return -1
			}
			return 1
		},
	)
	if len(backupNames) > 0 {
		return backupNames[0], nil
	}
	return "", nil
}

func (v *Validate) getPreviousMetadata(ctx context.Context, dumpId string) (*storageDto.Metadata, error) {

	st := v.mainSt.SubStorage(dumpId, true)

	f, err := st.GetObject(ctx, MetadataJsonFileName)
	if err != nil {
		return nil, fmt.Errorf("cannot open metadata file: %w", err)
	}
	defer f.Close()

	previousMetadata := &storageDto.Metadata{}

	if err = json.NewDecoder(f).Decode(&previousMetadata); err != nil {
		return nil, fmt.Errorf("cannot decode metadata file: %w", err)
	}
	return previousMetadata, nil
}

func findTableBySchemaAndName(Transformations []*domains.Table, schemaName, tableName string) (*domains.Table, error) {
	var foundTable *domains.Table
	for _, t := range Transformations {
		if t.Schema == schemaName && t.Name == tableName {
			foundTable = t
			break
		}
		if schemaName == "" && t.Name == tableName {
			if foundTable != nil {
				return nil, fmt.Errorf("wrong \"validate_table\" value: unable uniqually identify table \"%s\": sepcify schema name", tableName)
			}
			foundTable = t
		}
	}
	if foundTable == nil {
		errMsg := fmt.Sprintf("table %s is not found in transformation config", tableName)
		if schemaName != "" && tableName != "" {
			errMsg = fmt.Sprintf("table %s.%s is not found in transformation config", schemaName, tableName)
		}
		return nil, fmt.Errorf("unable to find table from \"validate_table\" parameter: %s", errMsg)
	}
	return foundTable, nil
}

func parseTableName(name string) (tableName string, schemaName string, err error) {
	parts := strings.Split(name, ".")

	if len(parts) > 2 {
		return "", "", fmt.Errorf("wrong \"validate_table\" format \"%s\": value has %d coma symbols (.)", name, len(parts))
	} else if len(parts) == 2 {
		schemaName = parts[0]
		tableName = parts[1]
	} else {
		tableName = parts[0]
	}
	return
}
