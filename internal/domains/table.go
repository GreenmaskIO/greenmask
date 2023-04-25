package domains

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"io"
	"strings"

	"github.com/rs/zerolog/log"
)

var TableDataDesc = "TABLE DATA"

type Table struct {
	Schema       string   `mapstructure:"schema"`
	Name         string   `mapstructure:"name"`
	Columns      []Column `mapstructure:"columns"`
	HasMasker    bool
	Oid          int
	Owner        string
	DumpId       int32
	Dependencies []int32
}

func (t *Table) TransformTuple(data []byte) ([]byte, error) {
	if !t.HasMasker {
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
