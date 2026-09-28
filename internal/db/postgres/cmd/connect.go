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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
)

// newConnConfig parses dsn and attaches onNotice. pgx.Connect gives no access to
// the config, and without OnNotice pgx silently drops server notices.
func newConnConfig(dsn string, onNotice pgconn.NoticeHandler) (*pgx.ConnConfig, error) {
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot parse connection string: %w", err)
	}
	connCfg.OnNotice = onNotice
	return connCfg, nil
}

func connectWithNotices(ctx context.Context, dsn string, onNotice pgconn.NoticeHandler) (*pgx.Conn, error) {
	connCfg, err := newConnConfig(dsn, onNotice)
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, connCfg)
}

func openConn(ctx context.Context, dsn string) (*pgx.Conn, error) {
	return connectWithNotices(ctx, dsn, newNoticeHandler(log.Logger, scriptNoticeLevels))
}

func openWorkerConn(ctx context.Context, dsn string, workerID int) (*pgx.Conn, error) {
	logger := log.With().Int(workerIDLogKey, workerID).Logger()
	return connectWithNotices(ctx, dsn, newNoticeHandler(logger, workerNoticeLevels))
}
