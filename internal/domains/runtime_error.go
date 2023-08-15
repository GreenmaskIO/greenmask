package domains

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

type RuntimeErrors []error

func (re RuntimeErrors) Error() string {
	return "runtime error"
}

func (re RuntimeErrors) IsFatal() bool {
	return slices.ContainsFunc(re, func(err error) bool {
		switch v := err.(type) {
		case *RuntimeError:
			return v.Level == zerolog.ErrorLevel
		default:
			return true
		}
	})
}

func (re RuntimeErrors) LogErrors() {
	for _, err := range re {
		switch v := err.(type) {
		case *RuntimeError:
			v.Log()
		default:
			log.Err(err).
				Msgf("internal error")
		}
	}
}

type RuntimeError struct {
	Msg   string                 `json:"msg,omitempty"`
	Err   error                  `json:"err,omitempty"`
	Level zerolog.Level          `json:"level,omitempty"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
}

func NewRuntimeError() *RuntimeError {
	return &RuntimeError{
		Level: zerolog.WarnLevel,
		Meta:  map[string]interface{}{},
	}
}

func (re *RuntimeError) Error() string {
	if re.Err != nil {
		if re.Msg == "" {
			return re.Err.Error()
		}
		return fmt.Sprintf("%s: %s", re.Msg, re.Err)
	} else {
		return re.Err.Error()
	}
}

func (re *RuntimeError) Unwrap() error {
	return re.Err
}

func (re *RuntimeError) SetMsg(msg string) *RuntimeError {
	re.Msg = msg
	return re
}

func (re *RuntimeError) SetErr(err error) *RuntimeError {
	re.Err = err
	return re
}

func (re *RuntimeError) SetLevel(level zerolog.Level) *RuntimeError {
	re.Level = level
	return re
}

func (re *RuntimeError) AddMeta(key string, value interface{}) *RuntimeError {
	re.Meta[key] = value
	return re
}

func (re *RuntimeError) LogEvent(event *zerolog.Event) *zerolog.Event {
	for k, v := range re.Meta {
		switch s := v.(type) {
		case string:
			event = event.Str(k, s)
		}
	}
	if re.Err != nil {
		event.Err(re.Err)
	}

	event.Msg(re.Msg)
	return event
}

func (re *RuntimeError) Log() *zerolog.Event {
	event := log.WithLevel(re.Level)
	for k, v := range re.Meta {
		switch s := v.(type) {
		case string:
			event = event.Str(k, s)
		}
	}
	if re.Err != nil {
		event.Err(re.Err)
	}
	event.Msg(re.Msg)
	return event
}
