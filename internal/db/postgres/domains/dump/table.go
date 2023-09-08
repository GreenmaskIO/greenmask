package dump

import (
	"errors"
	"fmt"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

type Table struct {
	*transformers.Table
	Query                string
	Owner                string
	RelKind              rune
	RootPtSchema         string
	RootPtName           string
	LoadViaPartitionRoot bool
	RootOid              transformers.Oid
	Transformers         []transformers.Transformer
	Dependencies         []int32
	DumpId               int32
	OriginalSize         int64
	CompressedSize       int64
	ExcludeData          bool
	Driver               *transformers.Driver
}

func (t *Table) SetDumpId(dumpId int32) {
	if dumpId == 0 {
		panic("dumpId cannot be 0")
	}
	t.DumpId = dumpId
}

func (t *Table) Entry() (*toc.Entry, error) {
	if t.Table == nil {
		return nil, fmt.Errorf("table is nil")
	}
	if t.Oid == 0 {
		return nil, errors.New("oid cannot be 0")
	}
	if t.Schema == "" {
		return nil, errors.New("table schema name cannot be empty")
	}
	if t.Name == "" {
		return nil, errors.New("table name cannot be empty")
	}

	columns := make([]string, 0, len(t.Columns))

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
		DumpId:       t.DumpId,
		Section:      toc.SectionData,
		HadDumper:    1,
		Tag:          &name,
		Namespace:    &schema,
		Owner:        &owner,
		Desc:         &toc.TableDataDesc,
		CopyStmt:     &copyStmt,
		Dependencies: dependencies,
		NDeps:        int32(len(dependencies)),
		FileName:     &fileName,
	}, nil
}

func (t *Table) GetCopyFromStatement() (string, error) {
	query := fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT WITH (FORMAT CSV, NULL '\\N')", t.Schema, t.Name)
	if t.Query != "" {
		query = fmt.Sprintf("COPY (%s) TO STDOUT WITH (FORMAT CSV, NULL '\\N')", t.Query)
	}
	return query, nil
}
