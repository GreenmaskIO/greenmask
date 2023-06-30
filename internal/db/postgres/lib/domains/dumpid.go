package domains

import "sync/atomic"

type DumpId int32

func (di *DumpId) GetDumpId() DumpId {
	atomic.AddInt32((*int32)(di), 1)
	return *di
}
