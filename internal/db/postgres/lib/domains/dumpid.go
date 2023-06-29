package domains

import "sync/atomic"

type DumpIdSequence int32

func (di *DumpIdSequence) GetDumpId() DumpIdSequence {
	atomic.AddInt32((*int32)(di), 1)
	return *di
}
