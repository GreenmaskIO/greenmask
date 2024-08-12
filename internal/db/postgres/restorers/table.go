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

package restorers

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/greenmaskio/greenmask/internal/utils/pgerrors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
)

const DefaultBufferSize = 1024 * 1024

type TableRestorer struct {
	Entry       *toc.Entry
	St          storages.Storager
	exitOnError bool
}

func NewTableRestorer(entry *toc.Entry, st storages.Storager, exitOnError bool) *TableRestorer {
	return &TableRestorer{
		Entry:       entry,
		St:          st,
		exitOnError: exitOnError,
	}
}

func (td *TableRestorer) Execute(ctx context.Context, conn *pgx.Conn) error {
	// TODO: Add tests

	if td.Entry.FileName == nil {
		return fmt.Errorf("cannot get file name from toc Entry")
	}

	reader, err := td.St.GetObject(ctx, *td.Entry.FileName)
	if err != nil {
		return fmt.Errorf("cannot open dump file: %w", err)
	}
	defer func(reader io.ReadCloser) {
		if err := reader.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing dump file")
		}
	}(reader)
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}
	defer func(gz *gzip.Reader) {
		if err := gz.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing gzip reader")
		}
	}(gz)

	// Open new transaction for each task
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cannot start transaction (restoring %s): %w", td.DebugInfo(), err)
	}

	log.Debug().Str("copyStmt", *td.Entry.CopyStmt).Msgf("performing pgcopy statement")
	f := tx.Conn().PgConn().Frontend()

	if err = td.restoreCopy(ctx, f, gz); err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			log.Warn().
				Err(txErr).
				Str("objectName", td.DebugInfo()).
				Msg("cannot rollback transaction")
		}
		if td.exitOnError {
			return fmt.Errorf("unable to restore table: %w", err)
		}
		log.Warn().Err(err).Msg("unable to restore table")
		return nil
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("cannot commit transaction (restoring %s): %w", td.DebugInfo(), err)
	}

	return nil
}

func (td *TableRestorer) restoreCopy(ctx context.Context, f *pgproto3.Frontend, r io.Reader) error {
	if err := td.initCopy(ctx, f); err != nil {
		return fmt.Errorf("error initializing pgcopy: %w", err)
	}

	if err := td.streamCopyData(ctx, f, r); err != nil {
		return fmt.Errorf("error streaming pgcopy data: %w", err)
	}

	if err := td.postStreamingHandle(ctx, f); err != nil {
		return fmt.Errorf("error post streaming handling: %w", err)
	}
	return nil
}

func (td *TableRestorer) initCopy(ctx context.Context, f *pgproto3.Frontend) error {
	err := sendMessage(f, &pgproto3.Query{String: *td.Entry.CopyStmt})
	if err != nil {
		return fmt.Errorf("error sending Query message: %w", err)
	}

	// Prepare for streaming the pgcopy data
	process := true
	for process {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := f.Receive()
		if err != nil {
			return fmt.Errorf("unable to perform pgcopy query: %w", err)
		}
		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			process = false
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("error from postgres connection: %w", pgerrors.NewPgError(v))
		default:
			return fmt.Errorf("unknown message %+v", v)
		}
	}
	return nil
}

func (td *TableRestorer) streamCopyData(ctx context.Context, f *pgproto3.Frontend, r io.Reader) error {
	// Streaming pgcopy data from table dump

	buf := make([]byte, DefaultBufferSize)
	for {
		var n int
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := r.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				completionErr := sendMessage(f, &pgproto3.CopyDone{})
				if completionErr != nil {
					return fmt.Errorf("error sending CopyDone message: %w", err)
				}
				break
			}
			return fmt.Errorf("error readimg from table dump: %w", err)
		}

		err = sendMessage(f, &pgproto3.CopyData{Data: buf[:n]})
		if err != nil {
			return fmt.Errorf("error sending DopyData message: %w", err)
		}
	}
	return nil
}

func (td *TableRestorer) postStreamingHandle(ctx context.Context, f *pgproto3.Frontend) error {
	// Perform post streaming handling
	var mainErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:

		}
		msg, err := f.Receive()
		if err != nil {
			return fmt.Errorf("unable to perform pgcopy query: %w", err)
		}
		switch v := msg.(type) {
		case *pgproto3.CommandComplete:
		case *pgproto3.ReadyForQuery:
			return mainErr
		case *pgproto3.ErrorResponse:
			mainErr = fmt.Errorf("error from postgres connection: %w", pgerrors.NewPgError(v))
		default:
			return fmt.Errorf("unknown message %+v", v)
		}
	}
}

func (td *TableRestorer) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", *td.Entry.Namespace, *td.Entry.Tag)
}

// sendMessage - send a message to the PostgreSQL backend and flush a buffer
func sendMessage(frontend *pgproto3.Frontend, msg pgproto3.FrontendMessage) error {
	frontend.Send(msg)
	if err := frontend.Flush(); err != nil {
		return fmt.Errorf("error flushing pgx frontend buffer: %w", err)
	}
	return nil
}
