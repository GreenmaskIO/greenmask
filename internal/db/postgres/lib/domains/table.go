package domains

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

var TableDataDesc = "TABLE DATA"

type Table struct {
	Schema               string         `mapstructure:"schema"`
	Name                 string         `mapstructure:"name"`
	Columns              []Column       `mapstructure:"columns"`
	Query                string         `mapstructure:"query"`
	QueryTest            string         `mapstructure:"queryTest"`
	HasTransformer       bool           `json:"-" yaml:"-"`
	Oid                  int            `json:"-" yaml:"-"`
	Owner                string         `json:"-" yaml:"-"`
	RelKind              rune           `json:"-" yaml:"-"`
	RootPtName           string         `json:"-" yaml:"-"`
	RootPtSchema         string         `json:"-" yaml:"-"`
	ExcludeData          bool           `json:"-" yaml:"-"`
	DumpId               DumpIdSequence `json:"-" yaml:"-"`
	Dependencies         []int32        `json:"-" yaml:"-"`
	OriginalSize         int64          `json:"-" yaml:"-"`
	CompressedSize       int64          `json:"-" yaml:"-"`
	LoadViaPartitionRoot bool           `json:"-" yaml:"-"`
}

func (t *Table) TransformTuple(data []byte) ([]byte, error) {
	if !t.HasTransformer {
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
