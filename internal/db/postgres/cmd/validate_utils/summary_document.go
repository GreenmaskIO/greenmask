package validate_utils

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/olekukonko/tablewriter"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
)

// The cmd package defines the same values, but importing it here would be an
// import cycle; text_document.go does the same for its table formats.
const (
	jsonFormatName      = "json"
	summaryDiffModeName = "summary"
)

const (
	unchangedWarning  = "transformed column is unchanged in too many rows"
	unexpectedWarning = "column changed but no transformer affects it"
)

// columnCounts holds counts only. It never keeps a value, so nothing read
// from the database can reach the output.
type columnCounts struct {
	name        string
	transformed bool
	changed     uint64
	unchanged   uint64
	nullsKept   uint64
}

type columnSummary struct {
	Name           string  `json:"name"`
	Transformed    bool    `json:"transformed"`
	Changed        uint64  `json:"changed"`
	Unchanged      uint64  `json:"unchanged"`
	NullsKept      uint64  `json:"nulls_kept"`
	UnchangedRatio float64 `json:"unchanged_ratio"`
	Warning        string  `json:"warning,omitempty"`
}

type summaryDocumentResponse struct {
	Schema   string          `json:"schema"`
	Name     string          `json:"name"`
	DiffMode string          `json:"diff_mode"`
	Rows     uint64          `json:"rows"`
	Columns  []columnSummary `json:"columns"`
}

// SummaryDocument compares original and transformed rows and prints, per
// column, how many rows changed. It prints no original or transformed value
// and no primary key, so it can run where data may be read but not shown.
type SummaryDocument struct {
	table              *entries.Table
	format             string
	onlyTransformed    bool
	unchangedThreshold float64
	rows               uint64
	columns            []*columnCounts
}

// NewSummaryDocument returns a summary document. format is "json" or "text".
// unchangedThreshold is a percentage from 0 to 100: a transformed column that
// keeps more than this share of its non-NULL values gets a warning, so 100
// turns the warning off.
func NewSummaryDocument(
	table *entries.Table, format string, onlyTransformed bool, unchangedThreshold float64,
) *SummaryDocument {
	transformedColumns := getAffectedColumns(table)
	columns := make([]*columnCounts, len(table.Columns))
	for idx, c := range table.Columns {
		_, transformed := transformedColumns[c.Name]
		columns[idx] = &columnCounts{name: c.Name, transformed: transformed}
	}
	return &SummaryDocument{
		table:              table,
		format:             format,
		onlyTransformed:    onlyTransformed,
		unchangedThreshold: unchangedThreshold,
		columns:            columns,
	}
}

func (sd *SummaryDocument) Append(original, transformed *pgcopy.Row) error {
	for idx, c := range sd.columns {
		originalValue, err := original.GetColumn(idx)
		if err != nil {
			return fmt.Errorf("error getting column from original record: %w", err)
		}
		transformedValue, err := transformed.GetColumn(idx)
		if err != nil {
			return fmt.Errorf("error getting column from transformed record: %w", err)
		}
		switch {
		case !ValuesEqual(originalValue, transformedValue):
			c.changed++
		case originalValue.IsNull: // equal and NULL, so NULL on both sides
			c.nullsKept++
		default:
			c.unchanged++
		}
	}
	sd.rows++
	return nil
}

func (sd *SummaryDocument) summarize() []columnSummary {
	res := make([]columnSummary, 0, len(sd.columns))
	for _, c := range sd.columns {
		s := columnSummary{
			Name:        c.name,
			Transformed: c.transformed,
			Changed:     c.changed,
			Unchanged:   c.unchanged,
			NullsKept:   c.nullsKept,
		}
		// NULLs kept as NULL are the usual behaviour and say nothing about
		// whether a transformer works, so they are left out of the ratio.
		compared := c.changed + c.unchanged
		if compared > 0 {
			s.UnchangedRatio = float64(c.unchanged) / float64(compared)
		}
		switch {
		// Compared as unchanged*100 > threshold*compared, so a ratio exactly
		// on the threshold never warns because of float rounding.
		case c.transformed && float64(c.unchanged)*100 > sd.unchangedThreshold*float64(compared):
			s.Warning = unchangedWarning
		case !c.transformed && c.changed > 0:
			s.Warning = unexpectedWarning
		}
		res = append(res, s)
	}
	return res
}

// visible drops, with --transformed-only, the columns that no transformer
// affects and that have nothing to warn about.
func (sd *SummaryDocument) visible(columns []columnSummary) []columnSummary {
	if !sd.onlyTransformed {
		return columns
	}
	res := make([]columnSummary, 0, len(columns))
	for _, c := range columns {
		if c.Transformed || c.Warning != "" {
			res = append(res, c)
		}
	}
	return res
}

func (sd *SummaryDocument) Print(w io.Writer) error {
	columns := sd.visible(sd.summarize())
	if sd.format == jsonFormatName {
		return json.NewEncoder(w).Encode(&summaryDocumentResponse{
			Schema:   sd.table.Schema,
			Name:     sd.table.Name,
			DiffMode: summaryDiffModeName,
			Rows:     sd.rows,
			Columns:  columns,
		})
	}

	_, err := fmt.Fprintf(
		w, "\n\n\t\"%s\".\"%s\" (%d rows compared, values not shown)\n", sd.table.Schema, sd.table.Name, sd.rows,
	)
	if err != nil {
		return fmt.Errorf("error writing title: %w", err)
	}
	prettyWriter := tablewriter.NewWriter(w)
	prettyWriter.SetHeader([]string{"Column", "Transformed", "Changed", "Unchanged", "NULLs kept", "Unchanged %", "Warning"})
	prettyWriter.SetAutoFormatHeaders(false)
	for _, c := range columns {
		transformed := "no"
		if c.Transformed {
			transformed = "yes"
		}
		prettyWriter.Append([]string{
			c.Name,
			transformed,
			strconv.FormatUint(c.Changed, 10),
			strconv.FormatUint(c.Unchanged, 10),
			strconv.FormatUint(c.NullsKept, 10),
			strconv.FormatFloat(c.UnchangedRatio*100, 'f', 1, 64),
			c.Warning,
		})
	}
	prettyWriter.Render()
	return nil
}
