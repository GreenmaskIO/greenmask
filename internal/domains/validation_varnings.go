package domains

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

var ValidationWarningLogSeverities = map[string]zerolog.Level{
	ErrorValidationSeverity:   zerolog.ErrorLevel,
	WarningValidationSeverity: zerolog.WarnLevel,
	InfoValidationSeverity:    zerolog.InfoLevel,
	DebugValidationSeverity:   zerolog.DebugLevel,
}

const (
	ErrorValidationSeverity   = "error"
	WarningValidationSeverity = "warning"
	InfoValidationSeverity    = "info"
	DebugValidationSeverity   = "debug"
)

type ValidationWarnings []*ValidationWarning

func (re ValidationWarnings) IsFatal() bool {
	return slices.ContainsFunc(re, func(warning *ValidationWarning) bool {
		return warning.Level == ErrorValidationSeverity
	})
}

type ValidationWarning struct {
	Msg   string                 `json:"msg,omitempty"`
	Level string                 `json:"level,omitempty"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
}

func NewValidationWarning() *ValidationWarning {
	return &ValidationWarning{
		Level: WarningValidationSeverity,
		Meta:  map[string]interface{}{},
	}
}

func (re *ValidationWarning) SetMsg(msg string) *ValidationWarning {
	re.Msg = msg
	return re
}

func (re *ValidationWarning) SetMsgf(msg string, args ...any) *ValidationWarning {
	re.Msg = fmt.Sprintf(msg, args...)
	return re
}

func (re *ValidationWarning) SetLevel(level string) *ValidationWarning {
	if _, ok := ValidationWarningLogSeverities[level]; !ok {
		panic(fmt.Sprintf("unknown validation level %s", level))
	}
	re.Level = level
	return re
}

func (re *ValidationWarning) AddMeta(key string, value interface{}) *ValidationWarning {
	re.Meta[key] = value
	return re
}

func (re *ValidationWarning) LogEvent(event *zerolog.Event) *zerolog.Event {
	for k, v := range re.Meta {
		switch s := v.(type) {
		case string:
			event = event.Str(k, s)
		}
	}

	event.Msg(re.Msg)
	return event
}

func (re *ValidationWarning) Log() *zerolog.Event {
	level, ok := ValidationWarningLogSeverities[re.Level]
	if !ok {
		panic(fmt.Sprintf("unknown validation level %s", re.Level))
	}
	event := log.WithLevel(level)
	for k, v := range re.Meta {
		switch s := v.(type) {
		case string:
			event = event.Str(k, s)
		}
	}
	event.Msg(re.Msg)
	return event
}
