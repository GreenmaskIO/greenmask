// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entries

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// escapeIdent escapes double-quote characters within a PostgreSQL identifier
// by doubling them. This is required when embedding identifier names in SQL
// strings using double-quote delimiters, e.g. "my""table".
func escapeIdent(name string) string {
	return strings.ReplaceAll(name, `"`, `""`)
}

// Table - godoc
type Table struct {
	*toolkit.Table
	Query                string
	Owner                string
	RelKind              rune
	LoadViaPartitionRoot bool
	// RootPtSchema - schema name of the root partition table uses in partitioned tables when LoadViaPartitionRoot
	// is set
	RootPtSchema string
	// RootPtName - name of the root partition table uses in partitioned tables when LoadViaPartitionRoot is set
	RootPtName          string
	RootPtOid           toolkit.Oid
	TransformersContext []*utils.TransformerContext
	Dependencies        []int32
	DumpId              int32
	OriginalSize        int64
	CompressedSize      int64
	//ExcludeData          bool
	Driver      *toolkit.Driver
	Scores      int64
	SubsetConds []string
	When        *toolkit.WhenCond
}

// HasCustomTransformer - check if table has custom transformer
func (t *Table) HasCustomTransformer() bool {
	return slices.ContainsFunc(t.TransformersContext, func(transformer *utils.TransformerContext) bool {
		_, ok := transformer.Transformer.(*custom.CmdTransformer)
		return ok
	})
}

// SetDumpId - set dump id for table - it uses in TOC entry identification
func (t *Table) SetDumpId(sequence *toc.DumpIdSequence) {
	if sequence == nil {
		panic("sequence cannot be nil")
	}
	t.DumpId = sequence.Next()
}

// Entry - create TOC entry for table. This uses in toc.dat entries generation
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
		if !column.IsGenerated {
			columns = append(columns, fmt.Sprintf(`"%s"`, escapeIdent(column.Name)))
		}
	}

	var query = "COPY \"%s\".\"%s\" (%s) FROM stdin;\n"
	var schemaName, tableName string
	if t.LoadViaPartitionRoot && t.RootPtSchema != "" && t.RootPtName != "" {
		schemaName = t.RootPtSchema
		tableName = t.RootPtName
	} else {
		schemaName = t.Schema
		tableName = t.Name
	}
	copyStmt := fmt.Sprintf(query, escapeIdent(schemaName), escapeIdent(tableName), strings.Join(columns, ", "))

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
		Defn:         new(string),
		DropStmt:     new(string),
	}, nil
}

// GetCopyFromStatement - get COPY FROM statement for table
func (t *Table) GetCopyFromStatement() (string, error) {
	// We could generate an explicit column list for the COPY statement, but it’s not necessary because, by default,
	// generated columns are excluded from the COPY operation.
	query := fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT", escapeIdent(t.Schema), escapeIdent(t.Name))
	if t.Query != "" {
		query = fmt.Sprintf("COPY (%s) TO STDOUT", t.Query)
	}
	return query, nil
}
