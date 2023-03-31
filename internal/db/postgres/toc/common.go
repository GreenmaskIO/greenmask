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

import (
	"sync/atomic"
)

var (
	TableDataDesc    = "TABLE DATA"
	LargeObjectsDesc = "BLOB"
	BlobsDesc        = "BLOBS"
	SequenceSetDesc  = "SEQUENCE SET"
	CommentDesc      = "COMMENT"
	AclDesc          = "ACL"
)

type Oid int32

type DumpIdSequence struct {
	current int32
}

func NewDumpSequence(initVal int32) *DumpIdSequence {
	return &DumpIdSequence{
		current: initVal,
	}
}

func (dis *DumpIdSequence) Next() int32 {
	atomic.AddInt32(&dis.current, 1)
	return dis.current
}
