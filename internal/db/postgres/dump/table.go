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

package dump

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

type Table struct {
	*toolkit.Table
	Query                string
	Owner                string
	RelKind              rune
	RootPtSchema         string
	RootPtName           string
	LoadViaPartitionRoot bool
	RootOid              toolkit.Oid
	TransformersContext  []*utils.TransformerContext
	Dependencies         []int32
	DumpId               int32
	OriginalSize         int64
	CompressedSize       int64
	ExcludeData          bool
	Driver               *toolkit.Driver
	// ValidateLimitedRecords - perform dumping and transformation only for N records and exit
	ValidateLimitedRecords uint64
}

func (t *Table) HasCustomTransformer() bool {
	return slices.ContainsFunc(t.TransformersContext, func(transformer *utils.TransformerContext) bool {
		_, ok := transformer.Transformer.(*custom.CmdTransformer)
		return ok
	})
}

func (t *Table) SetDumpId(sequence *toc.DumpIdSequence) {
	if sequence == nil {
		panic("sequence cannot be nil")
	}
	t.DumpId = sequence.Next()
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

	//var query = `COPY "%s"."%s" (%s) FROM stdin WITH (FORMAT CSV, NULL '\N');`
	var query = `COPY "%s"."%s" (%s) FROM stdin`
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
		Defn:         new(string),
		DropStmt:     new(string),
	}, nil
}

func (t *Table) GetCopyFromStatement() (string, error) {
	query := fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT", t.Schema, t.Name)
	if t.Query != "" {
		query = fmt.Sprintf("COPY (%s) TO STDOUT", t.Query)
	}
	return query, nil
}
