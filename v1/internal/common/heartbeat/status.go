package heartbeat

import (
	"errors"
	"fmt"
)

type Status string

const (
	StatusDone       Status = "done"
	StatusInProgress Status = "in-progress"
	StatusFailed     Status = "failed"
)

var (
	errInvalidStatus = errors.New("invalid status")
)

func (s Status) Validate() error {
	switch s {
	case StatusDone, StatusInProgress, StatusFailed:
		return nil
	default:
		return fmt.Errorf("validate status %s: %w", s, errInvalidStatus)
	}
}
