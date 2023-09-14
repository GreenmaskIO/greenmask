package toc

import (
	"sync/atomic"
)

var (
	TableDataDesc   = "TABLE DATA"
	LargeObjectDesc = "BLOBS"
	SequenceSetDesc = "SEQUENCE SET"
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
