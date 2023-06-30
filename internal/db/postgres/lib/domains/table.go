package domains

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

var TableDataDesc = "TABLE DATA"

type TableMeta struct {
	// Data that uses only for TocEntry
	// DumpId - unique int value for the table instance
	DumpId DumpId `json:"-" yaml:"-"`
	// Dependencies - list of the table dependencies that must be delivered first
	Dependencies []int32 `json:"-" yaml:"-"`

	// Total metadata that changing dump behaviour
	// Oid - pg_class.oid
	Oid Oid `json:"-" yaml:"-"`
	// Owner - table owner name
	Owner string `json:"-" yaml:"-"`
	// RelKind - relation type as in pg_class.relkind
	RelKind rune `json:"-" yaml:"-"`
	// Root - oid of the partition root
	Root         Oid    `json:"-" yaml:"-"`
	RootPtName   string `json:"-" yaml:"-"` // Deprecated
	RootPtSchema string `json:"-" yaml:"-"` // Deprecated
	// ExcludeData - exclude table data
	ExcludeData bool `json:"-" yaml:"-"`
	// OriginalSize - plain size of the COPY table data
	OriginalSize int64 `json:"-" yaml:"-"`
	// CompressedSize - compressed size of the COPY table data
	CompressedSize int64 `json:"-" yaml:"-"`
	// LoadViaPartitionRoot - generate COPY statement with load via partition root
	LoadViaPartitionRoot bool `json:"-" yaml:"-"`

	// Attributes that are important for Transformer validation
	IsPartition    bool  `json:"-" yaml:"-"`
	HasConstraints bool  `json:"-" yaml:"-"`
	ChecksCount    int16 `json:"-" yaml:"-"`
	HasRules       bool  `json:"-" yaml:"-"`
	HasTriggers    bool  `json:"-" yaml:"-"`
	// List of the constraints at the table
	Constraints []Constraint `json:"-" yaml:"-"`
}

type Table struct {
	TableMeta
	Schema string `mapstructure:"schema"`
	Name   string `mapstructure:"name"`
	// Columns - must be replaced to map instead map[string]Columns
	Columns    []Column          `mapstructure:"columns"` // Deprecated
	ColumnsMap map[string]Column `mapstructure:"columnsMap"`
	Query      string            `mapstructure:"query"`
	QueryTest  string            `mapstructure:"queryTest"`
	//HasTransformer       bool           `json:"-" yaml:"-"`
}

func (t *Table) HasTransformer() bool {
	return slices.ContainsFunc(t.Columns, func(column Column) bool {
		return column.Transformer != nil
	})
}

func (t *Table) TransformTuple(data []byte) ([]byte, error) {
	if !t.HasTransformer() {
		log.Warn().Msgf("called transformer for table %s.%s though it is not defined in config. maybe bug", t.Schema, t.Name)
		return data, nil
	}
	lineReader := csv.NewReader(bytes.NewReader(data))
	lineReader.Comma = '\t'
	values, err := lineReader.Read()
	if err != nil {
		return nil, fmt.Errorf("cannot read dump line: %w", err)
	}

	record := make([]string, 0, len(t.Columns))
	for idx, column := range t.Columns {
		transformedValue := values[idx]
		if column.TransformConf.Name != "" {
			transformedValue, err = column.Transformer.Transform(values[idx])
			if err != nil {
				return nil, fmt.Errorf("transformer %s error: %w", column.TransformConf.Name, err)
			}
		}
		record = append(record, transformedValue)
	}

	buf := bytes.Buffer{}
	lineWriter := csv.NewWriter(&buf)
	lineWriter.Comma = '\t'
	if err = lineWriter.Write(record); err != nil {
		return nil, fmt.Errorf("unnable to write line: %w", err)
	}
	lineWriter.Flush()

	res, err := io.ReadAll(&buf)
	if err != nil {
		return nil, fmt.Errorf("cannot read data from tsv reader: %w", err)
	}
	return res, nil
}

func (t *Table) GetCopyFromStatement() (string, error) {
	query := fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT", t.Schema, t.Name)
	if t.Query != "" {
		query = fmt.Sprintf("COPY (%s) TO STDOUT", t.Query)
	}
	return query, nil
}

func (t *Table) GetTocEntry() (*toc.Entry, error) {
	if t.Oid == 0 {
		return nil, errors.New("oid cannot be 0")
	}
	if t.Schema == "" {
		return nil, errors.New("schema name cannot be empty")
	}

	columns := make([]string, 0)

	for _, column := range t.Columns {
		columns = append(columns, fmt.Sprintf(`"%s"`, column.Name))
	}

	var query = `COPY "%s"."%s" (%s) FROM stdin;`
	var schemaName, tableName string
	if t.LoadViaPartitionRoot && t.RootPtSchema != "" && t.RootPtName != "" {
		schemaName = t.RootPtSchema
		tableName = t.RootPtName
	} else {
		schemaName = t.Schema
		tableName = t.Name
	}
	copyStmt := fmt.Sprintf(query, schemaName, tableName, strings.Join(columns, ", "))

	fileName := fmt.Sprintf("%d.dat.gz", t.DumpId)

	dependencies := make([]int32, 0)
	if len(t.Dependencies) != 0 {
		dependencies = t.Dependencies
	}

	name := fmt.Sprintf(`"%s"`, t.Name)
	schema := fmt.Sprintf(`"%s"`, t.Schema)
	owner := ""
	if t.Owner != "" {
		owner = fmt.Sprintf(`"%s"`, t.Owner)
	}

	return &toc.Entry{
		CatalogId: toc.CatalogId{
			Oid: toc.Oid(t.Oid),
		},
		DumpId:         int32(t.DumpId),
		Section:        toc.SectionData,
		HadDumper:      1,
		Tag:            &name,
		Namespace:      &schema,
		Owner:          &owner,
		Desc:           &TableDataDesc,
		CopyStmt:       &copyStmt,
		Dependencies:   dependencies,
		NDeps:          int32(len(dependencies)),
		FileName:       &fileName,
		OriginalSize:   t.OriginalSize,
		CompressedSize: t.CompressedSize,
	}, nil
}
