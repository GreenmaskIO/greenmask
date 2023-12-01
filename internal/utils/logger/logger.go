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

func SetLogLevel(logLevelStr string, logFormat string) error {

	var logLevel zerolog.Level
	switch logLevelStr {
	case zerolog.LevelDebugValue:
		logLevel = zerolog.DebugLevel
	case zerolog.LevelInfoValue:
		logLevel = zerolog.InfoLevel
	case zerolog.LevelWarnValue:
		logLevel = zerolog.WarnLevel
	default:
		return fmt.Errorf("unknown log level %s", logLevelStr)

	}

	var formatWriter io.Writer
	switch logFormat {
	case LogFormatJsonValue:
		formatWriter = os.Stderr
	case LogFormatTextValue:
		formatWriter = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	}

	if logLevelStr == zerolog.LevelDebugValue {
		log.Logger = zerolog.New(formatWriter).
			Level(logLevel).
			With().
			Timestamp().
			Caller().
			Int("pid", os.Getpid()).Logger()
	} else {
		log.Logger = zerolog.New(formatWriter).
			Level(logLevel).
			With().
			Timestamp().
			Logger()
	}
	return nil
}
