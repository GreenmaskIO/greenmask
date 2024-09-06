package delete

import "time"

type StorageResponse struct {
	Valid           []*Dump
	Failed          []*Dump
	UnknownOrFailed []*Dump
}

type Dump struct {
	DumpId   string
	Date     time.Time
	Status   string
	Database string
}
