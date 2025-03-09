package utils

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/internal/utils/testutils"
)

type connectorSuite struct {
	testutils.PgContainerSuite
}

func TestRestorers(t *testing.T) {
	suite.Run(t, new(connectorSuite))
}

func (s *connectorSuite) Test_connectorSuite_WithTx() {
	ctx := context.Background()
	conn, err := s.PgContainerSuite.GetConnection(ctx)
	s.Require().NoError(err)
	pgConn := NewPGConn(conn)
	s.Run("check commit", func() {
		err := pgConn.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "CREATE TABLE _test_table_commit (id SERIAL PRIMARY KEY, name TEXT)")
			return err
		})
		s.Require().NoError(err)

		var relOid uint32
		err = conn.QueryRow(ctx, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '_test_table_commit'").
			Scan(&relOid)
		s.Require().NoError(err)
		s.Require().NotZero(relOid)
	})

	s.Run("check error", func() {
		err := pgConn.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "CREATE TABLE _test_table_rollback (id SERIAL PRIMARY KEY, name TEXT)")
			s.Require().NoError(err)
			return errors.New("some error")
		})
		s.Require().Error(err)

		var relOid uint32
		err = conn.QueryRow(ctx, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '_test_table_rollback'").
			Scan(&relOid)
		s.Require().ErrorIs(err, pgx.ErrNoRows)
	})
}
