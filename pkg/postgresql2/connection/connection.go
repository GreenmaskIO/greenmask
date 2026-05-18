package connection

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog/log"
)

type Option func(*pgxpool.Config)

func WithMaxConns(n int32) Option {
	return func(cfg *pgxpool.Config) {
		cfg.MaxConns = n
	}
}

func WithMinConns(n int32) Option {
	return func(cfg *pgxpool.Config) {
		cfg.MinConns = n
	}
}

// WithConns sets both MinConns and MaxConns to n, creating a fixed-size pool.
func WithConns(n int32) Option {
	return func(cfg *pgxpool.Config) {
		cfg.MinConns = n
		cfg.MaxConns = n
	}
}

type Connection struct {
	pool *pgxpool.Pool
	db   *sql.DB
}

func NewConnection(ctx context.Context, connString string, opts ...Option) (*Connection, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}
	for _, o := range opts {
		o(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Connection{
		pool: pool,
		db:   stdlib.OpenDBFromPool(pool),
	}, nil
}

func (c *Connection) Pool() *pgxpool.Pool {
	return c.pool
}

func (c *Connection) Close() {
	_ = c.db.Close()
	c.pool.Close()
}

func (c *Connection) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

func (c *Connection) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

func (c *Connection) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

type txBehavior int

const (
	txBehaviorRollback txBehavior = iota
	txBehaviorCommit
)

type FrontendOption func(*frontendConfig)

type frontendConfig struct {
	txBehavior txBehavior
}

// WithCommitTransaction commits the transaction when the callback returns nil.
// On callback error the transaction is always rolled back.
func WithCommitTransaction() FrontendOption {
	return func(cfg *frontendConfig) {
		cfg.txBehavior = txBehaviorCommit
	}
}

// WithRollbackTransaction always rolls back the transaction after the callback.
// This is the default.
func WithRollbackTransaction() FrontendOption {
	return func(cfg *frontendConfig) {
		cfg.txBehavior = txBehaviorRollback
	}
}

// WithFrontend acquires a connection from the pool, begins a transaction, and
// calls fn with the low-level pgproto3 frontend for that connection. The
// transaction is committed or rolled back according to opts (rollback by default).
// On callback error the transaction is always rolled back.
func (c *Connection) WithFrontend(
	ctx context.Context,
	fn func(ctx context.Context, frontend *pgproto3.Frontend) error,
	opts ...FrontendOption,
) error {
	cfg := &frontendConfig{txBehavior: txBehaviorRollback}
	for _, o := range opts {
		o(cfg)
	}

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	frontend := tx.Conn().PgConn().Frontend()

	if fnErr := fn(ctx, frontend); fnErr != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			log.Ctx(ctx).Warn().Err(rbErr).Msg("rollback failed after callback error")
		}
		return fnErr
	}

	switch cfg.txBehavior {
	case txBehaviorCommit:
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
	default:
		if err := tx.Rollback(ctx); err != nil {
			return fmt.Errorf("rollback transaction: %w", err)
		}
	}
	return nil
}
