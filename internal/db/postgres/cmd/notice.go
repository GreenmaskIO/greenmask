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
	"github.com/rs/zerolog/log"
)

// logServerNotice logs a notice response at the level matching its PostgreSQL
// severity.
func logServerNotice(_ *pgconn.PgConn, n *pgconn.Notice) {
	if n == nil {
		return
	}

	// Severity is localised according to the server's lc_messages; the
	// unlocalised copy is the one worth matching on.
	severity := n.SeverityUnlocalized
	if severity == "" {
		severity = n.Severity
	}

	var ev *zerolog.Event
	switch severity {
	case "WARNING":
		ev = log.Warn()
	case "DEBUG":
		ev = log.Debug()
	default:
		// NOTICE, INFO and LOG are all informational.
		ev = log.Info()
	}

	ev = ev.Str("severity", severity)
	if n.Detail != "" {
		ev = ev.Str("detail", n.Detail)
	}
	if n.Hint != "" {
		ev = ev.Str("hint", n.Hint)
	}
	ev.Msg(n.Message)
}
