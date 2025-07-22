package models

import "time"

type ObjectID int

type ObjectKind string

const (
	ObjectKindTable ObjectKind = "table"
)

type DumpStat struct {
	RestorationContext RestorationContext                 `json:"restoration_context"`
	RestorationItems   map[TaskID]RestorationItem         `json:"restoration_items"`
	TaskStats          map[TaskID]TaskStat                `json:"task_stats"`
	TaskID2ObjectID    map[ObjectKind]map[TaskID]ObjectID `json:"task_id_2_object_id"`
	ObjectID2TaskID    map[ObjectKind]map[ObjectID]TaskID `json:"object_id_2_task_id"`
}

type ObjectStat struct {
	Engine          Engine     `json:"engine"`
	ID              ObjectID   `json:"id"`
	Kind            ObjectKind `json:"kind"`
	HumanReadableID string     `json:"human_readable_id"`
	OriginalSize    int64      `json:"original_size"`
	CompressedSize  int64      `json:"compressed_size"`
	Filename        string     `json:"filename"`
}

func NewObjectStat(
	engine Engine,
	kind ObjectKind,
	id ObjectID,
	humanReadableID string,
	size int64,
	compressedSize int64,
	fileName string,
) ObjectStat {
	return ObjectStat{
		Engine:          engine,
		Kind:            kind,
		ID:              id,
		HumanReadableID: humanReadableID,
		OriginalSize:    size,
		CompressedSize:  compressedSize,
		Filename:        fileName,
	}
}

type TaskStat struct {
	ObjectStat  ObjectStat    `json:"object_stat"`
	ID          TaskID        `json:"id"`
	Engine      Engine        `json:"engine"`
	Duration    time.Duration `json:"duration"`
	DumperType  string        `json:"dumper_type"`
	RecordCount int64         `json:"record_count"`
	// ObjectDefinition - definition of the object in JSON bytes.
	ObjectDefinition []byte `json:"table"`
}

func NewDumpStat(
	taskID TaskID,
	objectStat ObjectStat,
	duration time.Duration,
	dumperType string,
	recordCount int64,
	engine Engine,
	objectDefinition []byte,
) TaskStat {
	return TaskStat{
		ID:               taskID,
		ObjectStat:       objectStat,
		Duration:         duration,
		DumperType:       dumperType,
		RecordCount:      recordCount,
		Engine:           engine,
		ObjectDefinition: objectDefinition,
	}
}
