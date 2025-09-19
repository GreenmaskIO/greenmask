package models

import (
	"fmt"
	"strconv"
	"time"
)

var errEmptyDumpID = fmt.Errorf("dump id cannot be empty")

type DumpID string

const (
	DumpIDLatest DumpID = "latest"
)

func NewDumpID() DumpID {
	return DumpID(strconv.FormatInt(time.Now().UnixMilli(), 10))
}

func (d DumpID) Validate() error {
	if d == "" {
		return errEmptyDumpID
	}

	if d == DumpIDLatest {
		return nil
	}
	if _, err := strconv.ParseInt(string(d), 10, 64); err != nil {
		return fmt.Errorf("dump id must int or latest %s: %w", d, err)
	}
	return nil
}
