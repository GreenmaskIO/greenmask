package validate

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/pkg/csv"
)

type Format string

const (
	FormatNameJson Format = "json"
	FormatNameText Format = "text"

	MetadataJsonFileName = "metadata.json"
)

type Printer interface {
	Marshall() ([]byte, error)
	Append(original, transformed interfaces.RowDriver) error
}

func (f Format) Validate() error {
	switch f {
	case FormatNameJson, FormatNameText:
		return nil
	}
	return fmt.Errorf("validate format '%s': %w", f, commonmodels.ErrValueValidationFailed)
}

func getMetadata(ctx context.Context, st interfaces.Storager) (commonmodels.Metadata, error) {
	metaObj, err := st.GetObject(ctx, MetadataJsonFileName)
	if err != nil {
		return commonmodels.Metadata{}, fmt.Errorf("get metadata object: %w", err)
	}
	defer metaObj.Close()
	var metadata commonmodels.Metadata
	if err := json.NewDecoder(metaObj).Decode(&metadata); err != nil {
		return commonmodels.Metadata{}, fmt.Errorf("decode metadata json: %w", err)
	}
	return metadata, nil
}

func readOneRow(r *csv.Reader) (interfaces.RowDriver, error) {
	rec, err := r.Read()
	if err != nil {
		return nil, err
	}
	row := &CSVRecord{}
	if err := row.SetRow(rec); err != nil {
		return nil, err
	}
	return row, nil
}

func readTable(
	ctx context.Context,
	st interfaces.Storager,
	fileName string,
	withDiff bool,
	printer Printer,
) error {
	f, err := st.GetObject(ctx, fileName)
	if err != nil {
		return fmt.Errorf("get table object '%s': %w", fileName, err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	for {
		original, err := readOneRow(r)
		if err == io.EOF {
			break
		}
		transformed := original
		if withDiff {
			transformed, err = readOneRow(r)
			if err == io.EOF {
				break
			}
		}

		if err := printer.Append(original, transformed); err != nil {
			return fmt.Errorf("append record to printer from file '%s': %w", fileName, err)
		}
	}
	return nil
}

func printTable(
	ctx context.Context,
	st interfaces.Storager,
	withDiff bool,
	transformedOnly bool,
	format Format,
	tableFormat TableFormat,
	item commonmodels.RestorationItem,
	meta commonmodels.Metadata,
) error {
	if item.ObjectKind != commonmodels.ObjectKindTable {
		return fmt.Errorf(
			"print table: unsupported object kind '%s': %w", item.ObjectKind,
			commonmodels.ErrValueValidationFailed,
		)
	}
	var table commonmodels.Table
	if err := json.Unmarshal(item.ObjectDefinition, &table); err != nil {
		return fmt.Errorf("unmarshal table data: %w", err)
	}
	affectedColumns, ok := meta.DumpStat.RestorationContext.TableIDToAffectedColumns[commonmodels.ObjectID(table.ID)]
	if !ok {
		affectedColumns = []int{}
	}
	var printer Printer
	switch format {
	case FormatNameJson:
		printer = NewJsonDocument(table, affectedColumns, withDiff, transformedOnly)
	case FormatNameText:
		printer = NewTextDocument(table, affectedColumns, withDiff, transformedOnly, tableFormat)
	default:
		return fmt.Errorf("unsupported format '%s': %w", format, commonmodels.ErrValueValidationFailed)
	}
	if err := readTable(ctx, st, item.Filename, withDiff, printer); err != nil {
		return fmt.Errorf("read table data: %w", err)
	}
	output, err := printer.Marshall()
	if err != nil {
		return fmt.Errorf("marshall table data: %w", err)
	}
	fmt.Println(string(output))
	return nil
}

func PrintData(
	ctx context.Context,
	st interfaces.Storager,
	cfg *config.Config,
) error {
	format := Format(cfg.Validate.Format)
	if err := format.Validate(); err != nil {
		return fmt.Errorf("validate format: %w", err)
	}
	tableFormat := TableFormat(cfg.Validate.TableFormat)
	if format == FormatNameText {
		if err := tableFormat.Validate(); err != nil {
			return fmt.Errorf("validate table format: %w", err)
		}
	}

	meta, err := getMetadata(ctx, st)
	if err != nil {
		return fmt.Errorf("get metadata: %w", err)
	}
	items := make([]commonmodels.RestorationItem, 0, len(meta.DumpStat.RestorationItems))
	for _, item := range meta.DumpStat.RestorationItems {
		items = append(items, item)
	}
	slices.SortFunc(items, func(a, b commonmodels.RestorationItem) int {
		return cmp.Compare(a.ObjectID, b.ObjectID)
	})

	for _, item := range items {
		if item.ObjectKind != commonmodels.ObjectKindTable {
			continue
		}
		if err := printTable(
			ctx,
			st,
			cfg.Validate.Diff,
			cfg.Validate.OnlyTransformed,
			format,
			tableFormat,
			item,
			meta,
		); err != nil {
			return fmt.Errorf("print table data for item '%s': %w", item.Filename, err)
		}
	}
	return nil
}
