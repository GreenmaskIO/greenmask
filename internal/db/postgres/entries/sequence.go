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
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

type Sequence struct {
	Oid          toc.Oid
	Schema       string
	Name         string
	Owner        string
	IsCalled     bool
	DumpId       int32
	Dependencies []int32
	LastValue    int64
}

func (s *Sequence) SetDumpId(sequence *toc.DumpIdSequence) {
	if sequence == nil {
		panic("sequence cannot be nil")
	}
	s.DumpId = sequence.Next()
}

func (s *Sequence) Entry() (*toc.Entry, error) {
	isCalled := "true"
	if !s.IsCalled {
		isCalled = "false"
	}
	statement := fmt.Sprintf(`SELECT pg_catalog.setval('"%s"."%s"', %d, %s)`, s.Schema, s.Name, s.LastValue, isCalled)

	name := fmt.Sprintf(`"%s"`, s.Name)
	schema := fmt.Sprintf(`"%s"`, s.Schema)
	owner := ""
	if s.Owner != "" {
		owner = fmt.Sprintf(`"%s"`, s.Owner)
	}

	return &toc.Entry{
		// TODO: CatalogId is setting as 0. Ensure that it's fine
		CatalogId:    toc.CatalogId{},
		DumpId:       s.DumpId,
		Section:      toc.SectionData,
		HadDumper:    0,
		Tag:          &name,
		Namespace:    &schema,
		Owner:        &owner,
		Desc:         &toc.SequenceSetDesc,
		Defn:         &statement,
		Dependencies: s.Dependencies,
		NDeps:        int32(len(s.Dependencies)),
		FileName:     new(string),
		DropStmt:     new(string),
	}, nil
}
