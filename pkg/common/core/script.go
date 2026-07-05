package core

import (
	"fmt"
	"strings"
)

type ScriptEventType string

const (
	ScriptEventTypeBefore ScriptEventType = "before"
	ScriptEventTypeAfter  ScriptEventType = "after"
)

func (s ScriptEventType) Validate() error {
	switch s {
	case ScriptEventTypeBefore, ScriptEventTypeAfter:
		return nil
	default:
		return fmt.Errorf("value '%s': %w", s, ErrValueValidationFailed)
	}
}

type Script struct {
	Name      string          `mapstructure:"name"`
	Section   DumpSection     `mapstructure:"section"`
	When      ScriptEventType `mapstructure:"when"`
	Query     string          `mapstructure:"query"`
	QueryFile string          `mapstructure:"query_file"`
	Command   []string        `mapstructure:"command"`
}

func (s *Script) Validate() error {
	if err := s.When.Validate(); err != nil {
		return fmt.Errorf("validate 'when': %w", err)
	}

	values := []string{s.Query, s.QueryFile, strings.Join(s.Command, " ")}
	var count int
	for _, value := range values {
		if value != "" {
			count += 1
		}
	}
	if count == 0 {
		return fmt.Errorf("script '%s' has no values", s.Name)
	}
	if count > 1 {
		return fmt.Errorf("script '%s' has more than one value", s.Name)
	}
	return nil
}
