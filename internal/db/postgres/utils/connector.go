package utils

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

type PGConnector interface {
	WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
	GetConn() *pgx.Conn
}

// PGConn is a wrapper around pgx.Conn that allows to wrap logic in transactions
type PGConn struct {
	con *pgx.Conn
}

func NewPGConn(con *pgx.Conn) *PGConn {
	return &PGConn{
		con: con,
	}
}

func (p *PGConn) GetConn() *pgx.Conn {
	return p.con
}

func (p *PGConn) WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := p.con.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			log.Warn().
				Err(txErr).
				Msg("cannot rollback transaction")
		}
		return err
	}
	return tx.Commit(ctx)
}
