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
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

type LargeObject struct {
	CreateDumpId  int32
	CommentDumpId int32
	PermDumpId    int32
	TableOid      toc.Oid
	Oid           toc.Oid
	ACL           []*ACL
	DefaultACL    *ACL
	Comment       string
	Owner         string
}

func (lo *LargeObject) SetDumpId(sequence *toc.DumpIdSequence) {
	if sequence == nil {
		panic("sequence cannot be nil")
	}
	lo.CreateDumpId = sequence.Next()
	if lo.Comment != "" {
		lo.CommentDumpId = sequence.Next()
	}
	if len(lo.ACL) > 0 {
		lo.PermDumpId = sequence.Next()
	}
}

func (lo *LargeObject) CreateDdl() *toc.Entry {
	tag := fmt.Sprintf("%d", lo.Oid)
	defn := fmt.Sprintf("SELECT pg_catalog.lo_create('%d');\n", lo.Oid)
	dropStmt := fmt.Sprintf("SELECT pg_catalog.lo_unlink('%d');\n", lo.Oid)
	return &toc.Entry{
		CatalogId: toc.CatalogId{
			TableOid: lo.TableOid,
			Oid:      lo.Oid,
		},
		DumpId:   lo.CreateDumpId,
		Section:  toc.SectionPreData,
		Tag:      &tag,
		Owner:    &lo.Owner,
		Desc:     &toc.LargeObjectsDesc,
		Defn:     &defn,
		DropStmt: &dropStmt,
		FileName: new(string),
	}
}

func (lo *LargeObject) CommentDdl() *toc.Entry {
	if lo.Comment == "" {
		return nil
	}
	tag := fmt.Sprintf("LARGE OBJECT %d", lo.Oid)
	defn := fmt.Sprintf("COMMENT ON LARGE OBJECT 57344 IS '%s';\n", lo.Comment)
	return &toc.Entry{
		DumpId:       lo.CommentDumpId,
		Section:      toc.SectionNone,
		Tag:          &tag,
		Owner:        &lo.Owner,
		Desc:         &toc.CommentDesc,
		Defn:         &defn,
		Dependencies: []int32{lo.CreateDumpId},
		NDeps:        1,
		FileName:     new(string),
	}
}

func (lo *LargeObject) AclDdl() *toc.Entry {
	// Loop through the permissions
	// 1. If the default permission does not exist in the list, then create "REVOKE ALL FROM" statement
	// 2. Is grantor is not the owner use "SET SESSION AUTHORIZATION username;" and "RESET SESSION AUTHORIZATION;"
	// 3. Create GRANT TO

	if len(lo.ACL) == 0 {
		return nil
	}

	sb := &strings.Builder{}

	// Check to find that default ACL permission exists
	defaultAclIdx := slices.IndexFunc(lo.ACL, func(acl *ACL) bool {
		return acl.Value == lo.DefaultACL.Value
	})
	// If not exists than create revoke all statement
	if defaultAclIdx == -1 {
		sb.Write([]byte(fmt.Sprintf("REVOKE ALL ON LARGE OBJECT %d FROM \"%s\";\n", lo.Oid, lo.Owner)))
	}

	for idx, acl := range lo.ACL {
		if idx == defaultAclIdx {
			continue
		}

		if acl.Items[0].Grantor != lo.Owner {
			sb.Write([]byte(fmt.Sprintf("SET SESSION AUTHORIZATION \"%s\";\n", acl.Items[0].Grantor)))
		}

		for _, item := range acl.Items {
			withGrantOption := ""
			if item.Grantable {
				withGrantOption = " WITH GRANT OPTION"
			}
			grantStmt := fmt.Sprintf("GRANT %s ON LARGE OBJECT %d TO \"%s\"%s;\n",
				item.PrivilegeType, lo.Oid, item.Grantee, withGrantOption,
			)
			sb.Write([]byte(grantStmt))
		}

		if acl.Items[0].Grantor != lo.Owner {
			sb.Write([]byte("RESET SESSION AUTHORIZATION;\n"))
		}
	}

	tag := fmt.Sprintf("LARGE OBJECT %d", lo.Oid)
	defn := sb.String()

	return &toc.Entry{
		DumpId:       lo.PermDumpId,
		Section:      toc.SectionNone,
		Tag:          &tag,
		Owner:        &lo.Owner,
		Desc:         &toc.AclDesc,
		Defn:         &defn,
		Dependencies: []int32{lo.CreateDumpId},
		NDeps:        1,
		FileName:     new(string),
	}
}

type ACLItem struct {
	Grantor       string
	Grantee       string
	PrivilegeType string
	Grantable     bool
}

type ACL struct {
	Value string
	Items []*ACLItem
}

type Blobs struct {
	LargeObjects   []*LargeObject
	DumpId         int32
	Dependencies   []int32
	OriginalSize   int64
	CompressedSize int64
}

func (b *Blobs) GetAllDDLs() []*toc.Entry {
	var res []*toc.Entry
	for _, lo := range b.LargeObjects {
		e := lo.CreateDdl()
		res = append(res, e)
		e = lo.CommentDdl()
		if e != nil {
			res = append(res, e)
		}
		e = lo.AclDdl()
		if e != nil {
			res = append(res, e)
		}
	}
	return res
}

func (b *Blobs) SetDumpId(sequence *toc.DumpIdSequence) {
	if sequence == nil {
		panic("sequence cannot be nil")
	}
	b.DumpId = sequence.Next()
	for _, lo := range b.LargeObjects {
		lo.SetDumpId(sequence)
	}
}

func (b *Blobs) Entry() (*toc.Entry, error) {

	fileName := "blobs.toc"

	return &toc.Entry{
		CatalogId: toc.CatalogId{
			Oid:      0,
			TableOid: 0,
		},
		DumpId:       b.DumpId,
		Section:      toc.SectionData,
		Tag:          &toc.BlobsDesc,
		Desc:         &toc.BlobsDesc,
		Dependencies: nil,
		NDeps:        0,
		FileName:     &fileName,
		HadDumper:    1,
	}, nil
}
