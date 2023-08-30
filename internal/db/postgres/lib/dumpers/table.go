package dumpers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/toclib"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/utils/count_writer"
)

type TableDumper struct {
	table *toclib.Table
}

func NewTableDumper(table toclib.Table) *TableDumper {
	return &TableDumper{
		table: &table,
	}
}

func (td *TableDumper) Execute(ctx context.Context, tx pgx.Tx, st storage.Storager) (*toc.Entry, error) {
	datFile, err := st.GetWriter(ctx, fmt.Sprintf("%d.dat.gz", td.table.DumpId))
	if err != nil {
		return nil, fmt.Errorf("cannot open data file: %w", err)
	}
	defer datFile.Close()
	gz := count_writer.NewGzipWriter(datFile)
	defer gz.Close()

	frontend := tx.Conn().PgConn().Frontend()
	query, err := td.table.GetCopyFromStatement()
	log.Debug().
		Str("query", query).
		Msgf("dumping %s using copy query", td.DebugInfo())
	if err != nil {
		return nil, fmt.Errorf("cannot get COPY FROM statement: %w", err)
	}
	frontend.Send(&pgproto3.Query{
		String: query,
	})

	if err := frontend.Flush(); err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:

		}
		msg, err := frontend.Receive()
		if err != nil {
			return nil, fmt.Errorf("unable to perform copy query: %w", err)
		}
		switch v := msg.(type) {
		case *pgproto3.CopyOutResponse:
			// CopyOutResponse does not matter for us in TEXTUAL MODES
			// https://www.postgresql.org/docs/current/sql-copy.html
		case *pgproto3.CopyData:
			tupleData := v.Data
			if td.table.HasTransformer() {
				// TODO:
				// 	1. Use that place for implementing the pipeline dumper
				//  2. Implement function closure depending on the plain or transformation dump
				tupleData, err = td.table.TransformTuple(tupleData)
				if err != nil {
					return nil, fmt.Errorf("cannot convert plain data to tuple: %w", err)
				}
			}

			if _, err := gz.Write(tupleData); err != nil {
				return nil, fmt.Errorf("cannot store data into dat file: %w", err)
			}

		case *pgproto3.CopyDone:
		case *pgproto3.CommandComplete:
		case *pgproto3.ReadyForQuery:
			if err = gz.Flush(); err != nil {
				return nil, fmt.Errorf("cannot flush writer: %w", err)
			}
			td.table.OriginalSize = gz.ReceivedBytes()
			td.table.CompressedSize = gz.WrittenBytes()
			return td.table.GetTocEntry()
		case *pgproto3.ErrorResponse:
			return nil, fmt.Errorf("error from postgres connection msg = %s code=%s", v.Message, v.Code)
		default:
			return nil, fmt.Errorf("unknown backup message %+v", v)
		}
	}
}

func (td *TableDumper) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", td.table.Schema, td.table.Name)
}
