// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	LogFormatJsonValue = "json"
	LogFormatTextValue = "text"
)

var errUnknownLogFormat = fmt.Errorf("unknown log format")

// GetLogger builds a zerolog.Logger for the given level and format without
// mutating any global state.
func GetLogger(logLevelStr string, logFormat string) (zerolog.Logger, error) {

	var logLevel zerolog.Level
	switch logLevelStr {
	case zerolog.LevelDebugValue:
		logLevel = zerolog.DebugLevel
	case zerolog.LevelInfoValue:
		logLevel = zerolog.InfoLevel
	case zerolog.LevelWarnValue:
		logLevel = zerolog.WarnLevel
	default:
		return zerolog.Logger{}, fmt.Errorf("log level %s: %w", logLevelStr, errUnknownLogFormat)
	}

	var formatWriter io.Writer
	switch logFormat {
	case LogFormatJsonValue:
		formatWriter = os.Stderr
	case LogFormatTextValue:
		formatWriter = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	}

	if logLevelStr == zerolog.LevelDebugValue {
		return zerolog.New(formatWriter).
			Level(logLevel).
			With().
			Timestamp().
			Caller().
			Int("pid", os.Getpid()).Logger(), nil
	}
	return zerolog.New(formatWriter).
		Level(logLevel).
		With().
		Timestamp().
		Logger(), nil
}

// SetDefaultContextLogger builds a logger and installs it as both the global
// log.Logger and zerolog.DefaultContextLogger. Installing it as the default
// context logger makes log.Ctx(ctx) fall back to the configured logger when no
// logger is attached to the context (greenmask does not attach one), so the
// context-based logging idiom works everywhere instead of silently no-oping.
func SetDefaultContextLogger(logLevelStr string, logFormat string) error {
	l, err := GetLogger(logLevelStr, logFormat)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}
	zerolog.DefaultContextLogger = &l
	log.Logger = l
	return nil
}
