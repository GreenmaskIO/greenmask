package models

import "time"

type ObjectStat struct {
	Size           int64
	CompressedSize int64
	FileName       string
}

type DumpStat struct {
	ObjectStat
	Duration time.Duration
}
