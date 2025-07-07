package models

import (
	"strconv"
	"time"
)

type DumpID string

func NewDumpID() DumpID {
	return DumpID(strconv.FormatInt(time.Now().UnixMilli(), 10))
}
