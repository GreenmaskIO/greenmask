package toclib

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/toc"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var TableDataDesc = "TABLE DATA"

type Table struct {
	Schema string
	Name   string
	Query  string

	// Data that uses only for TocEntry
	// DumpId - unique int value for the table instance
	DumpId DumpId
	// Dependencies - list of the table dependencies that must be delivered first
	Dependencies []int32

	// Total metadata that changing dump behaviour
	// Oid - pg_class.oid
	Oid Oid
	// Owner - table owner name
	Owner string
	// RelKind - relation type as in pg_class.relkind
	RelKind rune
	// Root - oid of the partition root
	Root         Oid
	RootPtName   string // Deprecated
	RootPtSchema string // Deprecated
	// ExcludeData - exclude table data
	ExcludeData bool
	// OriginalSize - plain size of the COPY table data
	OriginalSize int64
	// CompressedSize - compressed size of the COPY table data
	CompressedSize int64
	// LoadViaPartitionRoot - generate COPY statement with load via partition root
	LoadViaPartitionRoot bool
	// Constraints - List of the constraints at the table
	Constraints []*Constraint
	// Columns - List of the table columns (attributes)
	Columns []*Column
	// Transformers - list of the initialised Transformers
	Transformers []domains.Transformer
}

func (t *Table) HasTransformer() bool {
	return len(t.Transformers) > 0
}

func (t *Table) TransformTuple(data []byte) ([]byte, error) {
	var err error

	for _, transformer := range t.Transformers {
		data, err = transformer.Transform(data)
		if err != nil {
			return nil, fmt.Errorf("transformer %s error: %w", transformer.GetName(), err)
		}
	}

	return data, nil
}

func (t *Table) GetCopyFromStatement() (string, error) {
	query := fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT WITH (FORMAT CSV, NULL '\\N')", t.Schema, t.Name)
	if t.Query != "" {
		query = fmt.Sprintf("COPY (%s) TO STDOUT WITH (FORMAT CSV, NULL '\\N')", t.Query)
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

	var query = `COPY "%s"."%s" (%s) FROM stdin WITH (FORMAT CSV, NULL '\N');`
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
