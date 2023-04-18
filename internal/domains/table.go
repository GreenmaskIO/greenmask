package domains

import (
	"errors"
	"fmt"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"strings"

	"github.com/rs/zerolog/log"
)

var TableDataDesc = "TABLE DATA"

type Table struct {
	Schema       string   `yaml:"schema"`
	Name         string   `yaml:"name"`
	Columns      []Column `yaml:"columns"`
	HasMasker    bool
	Oid          int
	Owner        string
	DumpId       int32
	Dependencies []int32
}

func (t *Table) MakeTuple(data []byte) (*Tuple, error) {
	tuple := &Tuple{
		Table:         t,
		OriginalTuple: data,
	}
	log.Debug().Msgf("%+v\n", tuple)
	return nil, errors.New("IMPLEMENT ME")
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
		columns = append(columns, column.Name)
	}

	copyStmt := fmt.Sprintf("COPY %s.%s (%s) FROM stdin;\n", t.Schema, t.Name, strings.Join(columns, ", "))
	fileName := fmt.Sprintf("%d.dat.gz", t.DumpId)

	return &toc.Entry{
		CatalogId: toc.CatalogId{
			Oid: toc.Oid(t.Oid),
		},
		DumpId:       t.DumpId,
		Section:      toc.SectionData,
		HadDumper:    1,
		Tag:          &t.Name,
		Namespace:    &t.Schema,
		Owner:        &t.Owner,
		Desc:         &TableDataDesc,
		CopyStmt:     &copyStmt,
		Dependencies: t.Dependencies,
		NDeps:        int32(len(t.Dependencies)),
		FileName:     &fileName,
	}, nil
}
