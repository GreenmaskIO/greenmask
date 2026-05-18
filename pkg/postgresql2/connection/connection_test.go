package connection

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

type connectionSuite struct {
	suite.Suite
	conn *Connection
}

func TestConnection(t *testing.T) {
	suite.Run(t, new(connectionSuite))
}

func (s *connectionSuite) SetupSuite() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	s.Require().NoError(err)

	conn, err := NewConnection(ctx, dsn, WithConns(3))
	s.Require().NoError(err)
	s.T().Cleanup(conn.Close)

	_, err = conn.ExecContext(ctx,
		`CREATE TABLE test_items (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`,
	)
	s.Require().NoError(err)
	_, err = conn.ExecContext(ctx,
		`INSERT INTO test_items (name) VALUES ('alpha'), ('beta'), ('gamma')`,
	)
	s.Require().NoError(err)

	s.conn = conn
}

func (s *connectionSuite) Test_WithConns_poolSize() {
	s.Equal(int32(3), s.conn.Pool().Stat().MaxConns())
}

func (s *connectionSuite) Test_ExecContext() {
	ctx := context.Background()
	res, err := s.conn.ExecContext(ctx,
		`INSERT INTO test_items (name) VALUES ('exec_test')`,
	)
	s.Require().NoError(err)
	affected, err := res.RowsAffected()
	s.Require().NoError(err)
	s.Equal(int64(1), affected)
}

func (s *connectionSuite) Test_QueryRowContext() {
	ctx := context.Background()
	var name string
	err := s.conn.QueryRowContext(ctx,
		`SELECT name FROM test_items WHERE name = $1`, "alpha",
	).Scan(&name)
	s.Require().NoError(err)
	s.Equal("alpha", name)
}

func (s *connectionSuite) Test_QueryContext() {
	ctx := context.Background()
	rows, err := s.conn.QueryContext(ctx,
		`SELECT name FROM test_items WHERE name = ANY($1)`,
		[]string{"alpha", "beta", "gamma"},
	)
	s.Require().NoError(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		s.Require().NoError(rows.Scan(&n))
		names = append(names, n)
	}
	s.Require().NoError(rows.Err())
	s.ElementsMatch([]string{"alpha", "beta", "gamma"}, names)
}

func (s *connectionSuite) Test_QueryRowContext_noRow() {
	ctx := context.Background()
	var name string
	err := s.conn.QueryRowContext(ctx,
		`SELECT name FROM test_items WHERE name = $1`, "no_such_row",
	).Scan(&name)
	s.ErrorContains(err, "no rows")
}

// Test_callerTimeout shows the intended pattern: callers wrap their context
// with a deadline before calling DB methods.
func (s *connectionSuite) Test_callerTimeout() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_items`).Scan(&n)
	s.Require().NoError(err)
	s.Greater(n, 0)
}

func (s *connectionSuite) Test_WithFrontend_rollback_default() {
	ctx := context.Background()

	// Insert a row via the frontend using a plain Query message.
	err := s.conn.WithFrontend(ctx, func(ctx context.Context, fe *pgproto3.Frontend) error {
		return sendSimpleQuery(fe, `INSERT INTO test_items (name) VALUES ('frontend_rollback')`)
	})
	s.Require().NoError(err)

	// Row must not be visible because the transaction was rolled back.
	var n int
	err = s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_items WHERE name = 'frontend_rollback'`).Scan(&n)
	s.Require().NoError(err)
	s.Equal(0, n)
}

func (s *connectionSuite) Test_WithFrontend_commit() {
	ctx := context.Background()

	err := s.conn.WithFrontend(ctx, func(ctx context.Context, fe *pgproto3.Frontend) error {
		return sendSimpleQuery(fe, `INSERT INTO test_items (name) VALUES ('frontend_commit')`)
	}, WithCommitTransaction())
	s.Require().NoError(err)

	var n int
	err = s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_items WHERE name = 'frontend_commit'`).Scan(&n)
	s.Require().NoError(err)
	s.Equal(1, n)
}

func (s *connectionSuite) Test_WithFrontend_callbackError_rollsback() {
	ctx := context.Background()
	callbackErr := fmt.Errorf("deliberate error")

	err := s.conn.WithFrontend(ctx, func(ctx context.Context, fe *pgproto3.Frontend) error {
		return callbackErr
	}, WithCommitTransaction())
	s.Require().ErrorIs(err, callbackErr)
}

// sendSimpleQuery sends a Query message via the raw frontend and drains
// responses until ReadyForQuery, mirroring how table.go uses the frontend.
func sendSimpleQuery(fe *pgproto3.Frontend, sql string) error {
	fe.Send(&pgproto3.Query{String: sql})
	if err := fe.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	for {
		msg, err := fe.Receive()
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}
		switch v := msg.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("postgres error: %s", v.Message)
		case *pgproto3.ReadyForQuery:
			return nil
		}
	}
}
