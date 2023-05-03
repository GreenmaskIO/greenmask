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
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
)

type TableRestorer struct {
	Entry *toc.Entry
	St    storage.Storager
}

func NewTableRestorer(entry *toc.Entry, st storage.Storager) *TableRestorer {
	return &TableRestorer{
		Entry: entry,
		St:    st,
	}
}

func (td *TableRestorer) Execute(ctx context.Context, tx pgx.Tx) error {

	if td.Entry.FileName == nil {
		return fmt.Errorf("cannot get file name from toc Entry")
	}

	reader, err := td.St.GetReader(ctx, *td.Entry.FileName)
	if err != nil {
		return fmt.Errorf("cannot open TSV file: %w", err)
	}
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}

	frontend := tx.Conn().PgConn().Frontend()
	frontend.Send(&pgproto3.Query{
		String: *td.Entry.CopyStmt,
	})

	if err = frontend.Flush(); err != nil {
		return err
	}

	process := true
	for process {
		msg, err := frontend.Receive()
		if err != nil {
			return fmt.Errorf("unable to perform copy query: %w", err)
		}
		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			log.Debug().Msgf("received CopyInResponse %+v", v)
			process = false
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("error from postgres connection msg = %s code=%s", v.Message, v.Code)
		default:
			return fmt.Errorf("unknown message %+v", v)
		}
	}

	buf := make([]byte, 1024)
	for {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := gz.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				frontend.Send(&pgproto3.CopyDone{})
				break
			}
			return fmt.Errorf("cannot read line from file: %w", err)
		}

		frontend.Send(&pgproto3.CopyData{
			Data: buf[:n],
		})
	}

	if err = frontend.Flush(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:

		}
		msg, err := frontend.Receive()
		if err != nil {
			return fmt.Errorf("unable to perform copy query: %w", err)
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
}

func (td *TableRestorer) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", *td.Entry.Namespace, *td.Entry.Tag)
}
