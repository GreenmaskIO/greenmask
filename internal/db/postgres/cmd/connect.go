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
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// newConnConfig parses dsn and attaches the notice handler to the resulting
// configuration.
//
// pgx.Connect is ParseConfig followed by ConnectConfig with no opportunity to
// touch the config in between, and OnNotice is only reachable through the
// config - so opening connections goes through here instead.
func newConnConfig(dsn string) (*pgx.ConnConfig, error) {
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot parse connection string: %w", err)
	}
	connCfg.OnNotice = logServerNotice
	return connCfg, nil
}

// openConn establishes a connection that forwards PostgreSQL notice responses
// to the log.
//
// Every connection in this package is opened through openConn on purpose. A
// connection made with pgx.Connect has no notice handler, so pgx receives
// NoticeResponse messages and drops them: anything the server reports - a
// RAISE NOTICE in a restore script, a warning during data load - disappears
// with no trace. Leaving a second, quieter way to connect in place would make
// that failure one careless call away.
func openConn(ctx context.Context, dsn string) (*pgx.Conn, error) {
	connCfg, err := newConnConfig(dsn)
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, connCfg)
}
