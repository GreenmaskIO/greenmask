package models

import "time"

type ObjectKind string

const (
	ObjectKindTable ObjectKind = "table"
)

type ObjectStat struct {
	ID             string
	Kind           ObjectKind
	OriginalSize   int64
	CompressedSize int64
	FileName       string
}

func NewObjectStat(
	kind ObjectKind,
	id string,
	size int64,
	compressedSize int64,
	fileName string,
) ObjectStat {
	return ObjectStat{
		Kind:           kind,
		ID:             id,
		OriginalSize:   size,
		CompressedSize: compressedSize,
		FileName:       fileName,
	}
}

type DumpStat struct {
	ObjectStat
	Duration   time.Duration
	DumperType string
}

func NewDumpStat(
	objectStat ObjectStat,
	duration time.Duration,
	dumperType string,
) DumpStat {
	return DumpStat{
		ObjectStat: objectStat,
		Duration:   duration,
		DumperType: dumperType,
	}
}
