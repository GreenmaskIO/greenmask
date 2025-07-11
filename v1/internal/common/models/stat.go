package models

import "time"

type ObjectStat struct {
	Size           int64
	CompressedSize int64
	FileName       string
}

func NewObjectStat(size int64, compressedSize int64, fileName string) ObjectStat {
	return ObjectStat{
		Size:           size,
		CompressedSize: compressedSize,
		FileName:       fileName,
	}
}

type DumpStat struct {
	ObjectStat
	Duration time.Duration
	Type     string
	ID       string
}

func NewDumpStat(
	objectStat ObjectStat,
	duration time.Duration,
	dumperType string,
	// id string,
) DumpStat {
	return DumpStat{
		ObjectStat: objectStat,
		Duration:   duration,
		Type:       dumperType,
		//ID: id,
	}
}
