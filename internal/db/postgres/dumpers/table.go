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

package dumpers

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/countwriter"
)

type TableDumper struct {
	table                *dump.Table
	recordNum            uint64
	validate             bool
	validateWithOriginal bool
}

func NewTableDumper(table *dump.Table, validate bool, validateWithOriginal bool) *TableDumper {
	return &TableDumper{
		table:                table,
		validate:             validate,
		validateWithOriginal: validateWithOriginal,
	}
}

func (td *TableDumper) Execute(ctx context.Context, tx pgx.Tx, st storages.Storager) (dump.Entry, error) {

	w, r := countwriter.NewGzipPipe()

	eg, gtx := errgroup.WithContext(ctx)

	// Writing goroutine
	eg.Go(
		func() error {
			defer func() {
				if err := r.Close(); err != nil {
					log.Warn().Err(err).Msg("error closing TableDumper reader")
				}
			}()
			err := st.PutObject(gtx, fmt.Sprintf("%d.dat.gz", td.table.DumpId), r)
			if err != nil {
				return fmt.Errorf("cannot write object: %w", err)
			}
			return nil
		},
	)

	// Dumping and transformation goroutine
	eg.Go(
		func() error {
			var pipeline Pipeliner
			var err error
			if len(td.table.Transformers) > 0 {
				if td.validate {
					pipeline, err = NewValidationPipeline(gtx, eg, td.table, w, td.validateWithOriginal)
					if err != nil {
						return fmt.Errorf("cannot initialize validation pipeline: %w", err)
					}
				} else {
					pipeline, err = NewTransformationPipeline(gtx, eg, td.table, w)
					if err != nil {
						return fmt.Errorf("cannot initialize transformation pipeline: %w", err)
					}
				}

			} else {
				pipeline = NewPlainDumpPipeline(td.table, w)
			}
			if err := pipeline.Init(gtx); err != nil {
				return fmt.Errorf("error initializing transformation pipeline: %w", err)
			}
			if err := td.process(gtx, tx, w, pipeline); err != nil {
				doneErr := pipeline.Done(gtx)
				if doneErr != nil {
					log.Warn().Err(err).Msg("error terminating transformation pipeline")
				}
				return fmt.Errorf("error processing table dump: %w", err)
			}
			log.Debug().Msg("transformation pipeline executed successfully")
			return pipeline.Done(gtx)
		},
	)

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	td.table.OriginalSize = w.GetCount()
	td.table.CompressedSize = r.GetCount()
	return td.table, nil
}

func (td *TableDumper) process(ctx context.Context, tx pgx.Tx, w io.WriteCloser, pipeline Pipeliner) (err error) {
	defer func() {
		if err := w.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing TableDumper writer")
		}
	}()

	frontend := tx.Conn().PgConn().Frontend()
	query, err := td.table.GetCopyFromStatement()
	log.Debug().
		Str("query", query).
		Msgf("dumping table %s.%s using pgcopy query", td.table.Schema, td.table.Name)
	if err != nil {
		return fmt.Errorf("cannot get COPY FROM statement: %w", err)
	}
	frontend.Send(&pgproto3.Query{
		String: query,
	})

	if err := frontend.Flush(); err != nil {
		return fmt.Errorf("error flushing pg frontend: %w", err)
	}

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
		case *pgproto3.CopyOutResponse:
			// CopyOutResponse does not matter for in TEXTUAL MODES
			// https://www.postgresql.org/docs/current/sql-copy.html
		case *pgproto3.CopyData:
			if err = pipeline.Dump(ctx, v.Data); err != nil {
				return fmt.Errorf("dump error: %w", err)
			}

			if td.validate {
				// Logic for validation limiter - exit after recordNum rows
				td.recordNum++
				if td.recordNum == td.table.ValidateLimitedRecords {
					return nil
				}
			}

		case *pgproto3.CopyDone:
		case *pgproto3.CommandComplete:
		case *pgproto3.ReadyForQuery:
			return pipeline.CompleteDump()
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("error from postgres connection msg = %s code=%s", v.Message, v.Code)
		default:
			return fmt.Errorf("unknown backup message %+v", v)
		}
	}
}

func (td *TableDumper) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", td.table.Schema, td.table.Name)
}
