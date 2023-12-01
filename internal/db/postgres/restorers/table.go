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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
)

const DefaultBufferSize = 1024 * 1024

type TableRestorer struct {
	Entry *toc.Entry
	St    storages.Storager
}

func NewTableRestorer(entry *toc.Entry, st storages.Storager) *TableRestorer {
	return &TableRestorer{
		Entry: entry,
		St:    st,
	}
}

func (td *TableRestorer) Execute(ctx context.Context, tx pgx.Tx) error {

	return func() error {
		if td.Entry.FileName == nil {
			return fmt.Errorf("cannot get file name from toc Entry")
		}

		reader, err := td.St.GetObject(ctx, *td.Entry.FileName)
		if err != nil {
			return fmt.Errorf("cannot open TSV file: %w", err)
		}
		defer reader.Close()
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("cannot create gzip reader: %w", err)
		}
		defer gz.Close()

		log.Debug().Str("copyStmt", *td.Entry.CopyStmt).Msgf("performing pgcopy statement")
		frontend := tx.Conn().PgConn().Frontend()
		frontend.Send(&pgproto3.Query{
			String: *td.Entry.CopyStmt,
		})

		if err = frontend.Flush(); err != nil {
			return err
		}

		// Prepare for streaming the pgcopy data
		process := true
		for process {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			msg, err := frontend.Receive()
			if err != nil {
				return fmt.Errorf("unable to perform pgcopy query: %w", err)
			}
			switch v := msg.(type) {
			case *pgproto3.CopyInResponse:
				process = false
			case *pgproto3.ErrorResponse:
				return fmt.Errorf("error from postgres connection msg = %s code=%s", v.Message, v.Code)
			default:
				return fmt.Errorf("unknown message %+v", v)
			}
		}

		// Streaming pgcopy data from table dump

		buf := make([]byte, DefaultBufferSize)
		for {
			var n int
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			n, err = gz.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					frontend.Send(&pgproto3.CopyDone{})
					break
				}
				return fmt.Errorf("error readimg from table dump: %w", err)
			}

			frontend.Send(&pgproto3.CopyData{
				Data: buf[:n],
			})
		}

		if err = frontend.Flush(); err != nil {
			return err
		}

		// Perform post streaming handling
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:

			}
			msg, err := frontend.Receive()
			if err != nil {
				return fmt.Errorf("unable to perform pgcopy query: %w", err)
			}
			switch v := msg.(type) {
			case *pgproto3.CommandComplete:
			case *pgproto3.ReadyForQuery:
				return nil
			case *pgproto3.ErrorResponse:
				return fmt.Errorf("error from postgres connection msg = %s code=%s", v.Message, v.Code)
			default:
				return fmt.Errorf("unknown message %+v", v)
			}
		}
	}()

}

func (td *TableRestorer) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", *td.Entry.Namespace, *td.Entry.Tag)
}
