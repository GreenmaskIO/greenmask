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

package utils

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

const (
	LogFormatJsonValue = "json"
	LogFormatTextValue = "text"
)

var errUnknownLogFormat = fmt.Errorf("unknown log format")

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

func SetDefaultContextLogger(logLevelStr string, logFormat string) error {
	logger, err := GetLogger(logLevelStr, logFormat)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}
	zerolog.DefaultContextLogger = &logger
	return nil
}
