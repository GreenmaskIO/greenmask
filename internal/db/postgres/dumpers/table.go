package dumpers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/utils/count_writer"
)

const DefaultBufSize = 1024 * 10

type TableDumper struct {
	table *dump.Table
}

func NewTableDumper(table *dump.Table) *TableDumper {
	return &TableDumper{
		table: table,
	}
}

func (td *TableDumper) Execute(ctx context.Context, tx pgx.Tx, st storage.Storager) (toc.EntryProducer, error) {
	var err error

	datFile, err := st.GetWriter(ctx, fmt.Sprintf("%d.dat.gz", td.table.DumpId))
	if err != nil {
		return nil, fmt.Errorf("cannot open data file: %w", err)
	}
	defer datFile.Close()
	gz := count_writer.NewGzipWriter(datFile)
	defer gz.Close()

	var pipeline Pipeliner

	if len(td.table.Transformers) > 0 {
		pipeline, err = NewTransformationPipeline(ctx, td.table, gz)
		if err != nil {
			return nil, fmt.Errorf("cannot initialize transformation pipeline: %w", err)
		}
	} else {
		pipeline = NewPlainDumpPipeline(td.table, gz)
	}

	frontend := tx.Conn().PgConn().Frontend()
	query, err := td.table.GetCopyFromStatement()
	log.Debug().
		Str("query", query).
		Msgf("dumping table %s.%s using copy query", td.table.Schema, td.table.Name)
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
			if err = pipeline.Dump(ctx, v.Data); err != nil {
				return nil, fmt.Errorf("dump error: %w", err)
			}

		case *pgproto3.CopyDone:
		case *pgproto3.CommandComplete:
		case *pgproto3.ReadyForQuery:
			if err = gz.Flush(); err != nil {
				return nil, fmt.Errorf("cannot flush writer: %w", err)
			}
			td.table.OriginalSize = gz.ReceivedBytes()
			td.table.CompressedSize = gz.WrittenBytes()
			return td.table, nil
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
