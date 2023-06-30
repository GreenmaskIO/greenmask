package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
)

func RunValidate(ctx context.Context, opt *pgdump.Options, tableConfig []domains.Table) error {

	dsn, err := d.pgDumpOptions.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cannot build connection string: %w", err)
	}

	conn, err := pgx.Connect(ctx, dsn)
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}

	_, errs := BuildTablesConfig(ctx, tx, tableConfig)
	if errs != nil {
		errs.LogErrors()
	}
	if errs.IsFatal() {
		return fmt.Errorf("fatal validation error")
	}

	defer tx.Rollback(ctx)
	return nil
}
