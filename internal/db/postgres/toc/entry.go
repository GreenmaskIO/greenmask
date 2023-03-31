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

package toc

const (
	SectionNone     int32 = iota + 1
	SectionPreData        /* stuff to be processed before data */
	SectionData           /* table data, large objects, LO comments */
	SectionPostData       /* stuff to be processed after data */
)

var SectionMap = map[int32]string{
	SectionNone:     "None",
	SectionPreData:  "PreData",
	SectionData:     "Data",
	SectionPostData: "PostData",
}

type CatalogId struct {
	TableOid Oid
	Oid      Oid
}

type Entry struct {
	CatalogId CatalogId
	DumpId    int32
	Section   int32
	HadDumper int32 /* Archiver was passed a dumper routine (used
	 * in restore) */
	Tag        *string /* index Tag */
	Namespace  *string /* null or empty string if not in a schema */
	Tablespace *string /* null if not in a Tablespace; empty string
	 * means use database default */
	Tableam      *string /* table access method, only for TABLE tags */
	Owner        *string
	Desc         *string
	Defn         *string
	DropStmt     *string
	CopyStmt     *string
	Dependencies []int32 /* dumpIds of objects this one depends on */
	NDeps        int32   /* number of Dependencies */
	FileName     *string

	DataDumper int32 /* Routine to dump data for object */

	/* working state while dumping/restoring */
	DataLength uint32 /* item's data size; 0 if none or unknown */

	//OriginalSize   int64
	//CompressedSize int64
}
