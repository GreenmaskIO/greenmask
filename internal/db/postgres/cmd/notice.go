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

package cmd

import (
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
)

const workerIDLogKey = "workerId"

type noticeLevels map[string]zerolog.Level

var scriptNoticeLevels = noticeLevels{
	"WARNING": zerolog.WarnLevel,
	"NOTICE":  zerolog.InfoLevel,
	"INFO":    zerolog.InfoLevel,
	"LOG":     zerolog.InfoLevel,
	"DEBUG":   zerolog.DebugLevel,
}

// workerNoticeLevels demotes informational notices, since row-level triggers may raise one per row.
var workerNoticeLevels = noticeLevels{
	"WARNING": zerolog.WarnLevel,
	"NOTICE":  zerolog.DebugLevel,
	"INFO":    zerolog.DebugLevel,
	"LOG":     zerolog.DebugLevel,
	"DEBUG":   zerolog.DebugLevel,
}

func (l noticeLevels) of(severity string) zerolog.Level {
	if level, ok := l[severity]; ok {
		return level
	}
	return zerolog.InfoLevel
}

func noticeSeverity(n *pgconn.Notice) string {
	if n.SeverityUnlocalized != "" {
		return n.SeverityUnlocalized
	}
	return n.Severity
}

func newNoticeHandler(logger zerolog.Logger, levels noticeLevels) pgconn.NoticeHandler {
	return func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if n == nil {
			return
		}

		severity := noticeSeverity(n)
		ev := logger.WithLevel(levels.of(severity)).Str("severity", severity)
		if n.Detail != "" {
			ev.Str("detail", n.Detail)
		}
		if n.Hint != "" {
			ev.Str("hint", n.Hint)
		}
		ev.Msg(n.Message)
	}
}
