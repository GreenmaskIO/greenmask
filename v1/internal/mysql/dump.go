package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/sqltocsv"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
	
)

type Dump struct {
	binPath                  string
	cfg                      *domains.Dump
	st                       storages.Storager
	dumpOptions              *DumpOptions
	schemaDumpSize           int64
	schemaDumpSizeCompressed int64
	tables                   []*Table
}

func NewDump(cfg *domains.Dump, st storages.Storager, binPath string) *Dump {
	do, ok := cfg.Options.(*DumpOptions)
	if !ok {
		panic("invalid options")
	}
	return &Dump{
		cfg:         cfg,
		st:          st,
		dumpOptions: do,
		binPath:     binPath,
	}
}

func (d *Dump) Run(ctx context.Context) error {
	conn, err := d.connect(ctx)
	if err != nil {
		return fmt.Errorf("cannot connect to database: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Warn().Err(err).Msg("cannot close connection")
		}
	}()
	tx, err := conn.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Warn().Err(err).Msg("cannot rollback transaction")
		}
	}()

	if err := d.introspect(ctx, tx); err != nil {
		return fmt.Errorf("cannot introspect database: %w", err)
	}
	if err := d.schemaOnlyDump(ctx); err != nil {
		return fmt.Errorf("cannot dump schema: %w", err)
	}
	if err := d.dataDump(ctx); err != nil {
		return fmt.Errorf("cannot dump data: %w", err)
	}
	if err := d.writeMetadata(ctx); err != nil {
		return fmt.Errorf("cannot write metadata: %w", err)
	}
	return nil
}

// implement
// schemaOnlyDump(ctx)
// dataDump(ctx)
// writeMetadata(ctx)
// introspect(ctx)

func (d *Dump) connect(ctx context.Context) (*sql.DB, error) {
	dsn := "admin:admin@tcp(localhost:3306)/playground?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot open connection: %w", err)
	}
	return db, nil
}

func (d *Dump) introspect(ctx context.Context, tx driver.Tx) error {
	return nil
}

func (d *Dump) schemaOnlyDump(ctx context.Context) error {
	params, err := d.dumpOptions.Params()
	if err != nil {
		return fmt.Errorf("cannot get dump params: %w", err)
	}

	w, r := ioutils.NewGzipPipe(false)
	eg, gtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := d.st.PutObject(gtx, "schema.sql", r); err != nil {
			return fmt.Errorf("cannot get object: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		defer w.Close()
		if err := cmdrunner.NewCmdRunner(d.binPath, params, w).Run(ctx); err != nil {
			return fmt.Errorf("cannot run mysqldump: %w", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (d *Dump) dataDump(ctx context.Context) error {
	for _, table := range d.tables {
		if err := d.runTableDump(ctx, table, nil); err != nil {
			return fmt.Errorf("cannot dump table: %w", err)
		}
	}
	return nil
}

func (d *Dump) runTableDump(ctx context.Context, t *Table, tx *sql.Tx) error {
	//
	rows, err := tx.Query("SELECT * FROM users WHERE something=72")
	if err != nil {
		return fmt.Errorf("cannot execute query: %w", err)
	}

	csvConverter := sqltocsv.New(rows)
	csvConverter.TimeFormat = time.RFC822
	//csvConverter.Write()
	return nil
}

func (d *Dump) writeMetadata(ctx context.Context) error {
	return nil
}
