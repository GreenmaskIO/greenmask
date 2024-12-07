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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/pgerrors"
	"github.com/greenmaskio/greenmask/internal/utils/reader"
)

const defaultBufferSize = 1024 * 1024

type TableRestorer struct {
	*restoreBase
}

func NewTableRestorer(
	entry *toc.Entry, st storages.Storager, opt *pgrestore.DataSectionSettings,
) *TableRestorer {
	return &TableRestorer{
		restoreBase: newRestoreBase(entry, st, opt),
	}
}

func (td *TableRestorer) GetEntry() *toc.Entry {
	return td.entry
}

func (td *TableRestorer) Execute(ctx context.Context, conn *pgx.Conn) error {
	// TODO: Add tests

	if td.entry.FileName == nil {
		return fmt.Errorf("cannot get file name from toc Entry")
	}

	r, err := td.getObject(ctx)
	if err != nil {
		return fmt.Errorf("cannot get storage object: %w", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Warn().
				Err(err).
				Str("objectName", td.DebugInfo()).
				Msg("cannot close storage object")
		}
	}()

	// Open new transaction for each task
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cannot start transaction (restoring %s): %w", td.DebugInfo(), err)
	}
	if err := td.setupTx(ctx, tx); err != nil {
		return fmt.Errorf("cannot setup transaction: %w", err)
	}

	log.Debug().
		Str("copyStmt", *td.entry.CopyStmt).
		Msgf("performing pgcopy statement")
	f := tx.Conn().PgConn().Frontend()

	if err = td.restoreCopy(ctx, f, r); err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			log.Warn().
				Err(txErr).
				Str("objectName", td.DebugInfo()).
				Msg("cannot rollback transaction")
		}
		if td.opt.ExitOnError {
			return fmt.Errorf("unable to restore table: %w", err)
		}
		log.Warn().
			Err(err).
			Str("objectName", td.DebugInfo()).
			Msg("unable to restore table")
		return nil
	}

	if err := td.resetTx(ctx, tx); err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			log.Warn().
				Err(txErr).
				Str("objectName", td.DebugInfo()).
				Msg("cannot rollback transaction")
		}
		if td.opt.ExitOnError {
			return fmt.Errorf("unable to reset transaction: %w", err)
		}
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

	if td.opt.BatchSize > 0 {
		if err := td.streamCopyDataByBatch(ctx, f, r); err != nil {
			return fmt.Errorf("error streaming pgcopy data: %w", err)
		}
	} else {
		if err := td.streamCopyData(ctx, f, r); err != nil {
			return fmt.Errorf("error streaming pgcopy data: %w", err)
		}
	}

	if err := td.postStreamingHandle(ctx, f); err != nil {
		return fmt.Errorf("error post streaming handling: %w", err)
	}
	return nil
}

func (td *TableRestorer) initCopy(ctx context.Context, f *pgproto3.Frontend) error {
	err := sendMessage(f, &pgproto3.Query{String: *td.entry.CopyStmt})
	if err != nil {
		return fmt.Errorf("error sending Query message: %w", err)
	}

	// Prepare for streaming the pgcopy data
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
		case *pgproto3.CopyInResponse:
			return nil
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("error from postgres connection: %w", pgerrors.NewPgError(v))
		default:
			return fmt.Errorf("unknown message %+v", v)
		}
	}
}

// streamCopyDataByBatch - stream pgcopy data from table dump in batches. It handles errors only on the end each batch
// If the batch size is reached it completes the batch and starts a new one. If an error occurs during the batch it
// stops immediately and returns the error
func (td *TableRestorer) streamCopyDataByBatch(ctx context.Context, f *pgproto3.Frontend, r io.Reader) (err error) {
	bi := bufio.NewReader(r)
	buf := make([]byte, defaultBufferSize)
	var lineNum int64
	for {
		buf, err = reader.ReadLine(bi, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error readimg from table dump: %w", err)
		}
		if isTerminationSeq(buf) {
			break
		}
		lineNum++
		buf = append(buf, '\n')

		err = sendMessage(f, &pgproto3.CopyData{Data: buf})
		if err != nil {
			return fmt.Errorf("error sending CopyData message: %w", err)
		}

		if lineNum%td.opt.BatchSize == 0 {
			if err = td.completeBatch(ctx, f); err != nil {
				return fmt.Errorf("error completing batch: %w", err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// streamCopyData - stream pgcopy data from table dump in classic way. It handles errors only on the end of the stream
func (td *TableRestorer) streamCopyData(ctx context.Context, f *pgproto3.Frontend, r io.Reader) error {
	// Streaming pgcopy data from table dump

	buf := make([]byte, defaultBufferSize)
	for {
		var n int

		n, err := r.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error readimg from table dump: %w", err)
		}

		err = sendMessage(f, &pgproto3.CopyData{Data: buf[:n]})
		if err != nil {
			return fmt.Errorf("error sending DopyData message: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// completeBatch - complete batch of pgcopy data and initiate new one
func (td *TableRestorer) completeBatch(ctx context.Context, f *pgproto3.Frontend) error {
	if err := td.postStreamingHandle(ctx, f); err != nil {
		return err
	}
	if err := td.initCopy(ctx, f); err != nil {
		return err
	}
	return nil
}

func (td *TableRestorer) postStreamingHandle(ctx context.Context, f *pgproto3.Frontend) error {
	// Perform post streaming handling
	err := sendMessage(f, &pgproto3.CopyDone{})
	if err != nil {
		return fmt.Errorf("error sending CopyDone message: %w", err)
	}
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

// sendMessage - send a message to the PostgreSQL backend and flush a buffer
func sendMessage(frontend *pgproto3.Frontend, msg pgproto3.FrontendMessage) error {
	frontend.Send(msg)
	if err := frontend.Flush(); err != nil {
		return fmt.Errorf("error flushing pgx frontend buffer: %w", err)
	}
	return nil
}
