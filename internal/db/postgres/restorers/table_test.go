package restorers

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/utils"
	"github.com/greenmaskio/greenmask/internal/utils/testutils"
)

func (s *restoresSuite) Test_TableRestorer_check_triggers_errors() {
	s.Run("check triggers causes error by default", func() {
		// Given
		ctx := context.Background()
		schemaName := "public"
		tableName := "orders"
		fileName := "test_table"
		copyStmt := "COPY orders (id, user_id, order_amount, raise_error) FROM stdin;"
		entry := &toc.Entry{
			Namespace: &schemaName,
			Tag:       &tableName,
			FileName:  &fileName,
			CopyStmt:  &copyStmt,
		}
		data := "3\t1\t100.50\tTest exception\n"
		buf := new(bytes.Buffer)
		gzData := gzip.NewWriter(buf)
		_, err := gzData.Write([]byte(data))
		s.Require().NoError(err)
		err = gzData.Flush()
		s.Require().NoError(err)
		err = gzData.Close()
		s.Require().NoError(err)
		objSrc := &readCloserMock{Buffer: buf}

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, mock.Anything).Return(objSrc, nil)
		opt := &pgrestore.DataSectionSettings{
			ExitOnError: true,
		}
		tr := NewTableRestorer(entry, st, opt)

		conn, err := s.GetConnectionWithUser(ctx, s.nonSuperUser, s.nonSuperUserPassword)
		s.Require().NoError(err)
		err = tr.Execute(ctx, utils.NewPGConn(conn))
		s.Require().ErrorContains(err, "Test exception  (code P0001)")
	})

	s.Run("disable triggers", func() {
		ctx := context.Background()
		schemaName := "public"
		tableName := "orders"
		fileName := "test_table"
		copyStmt := "COPY orders (id, user_id, order_amount, raise_error) FROM stdin;"
		entry := &toc.Entry{
			Namespace: &schemaName,
			Tag:       &tableName,
			FileName:  &fileName,
			CopyStmt:  &copyStmt,
		}
		data := "4\t1\t100.50\tTest exception\n"
		buf := new(bytes.Buffer)
		gzData := gzip.NewWriter(buf)
		_, err := gzData.Write([]byte(data))
		s.Require().NoError(err)
		err = gzData.Flush()
		s.Require().NoError(err)
		err = gzData.Close()
		s.Require().NoError(err)
		objSrc := &readCloserMock{Buffer: buf}

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, mock.Anything).Return(objSrc, nil)
		opt := &pgrestore.DataSectionSettings{
			ExitOnError:     true,
			DisableTriggers: true,
			SuperUser:       s.GetSuperUser(),
		}
		tr := NewTableRestorer(entry, st, opt)

		conn, err := s.GetConnectionWithUser(ctx, s.nonSuperUser, s.nonSuperUserPassword)
		s.Require().NoError(err)
		err = tr.Execute(ctx, utils.NewPGConn(conn))
		s.Require().NoError(err)
	})

	s.Run("session_replication_role is replica", func() {
		ctx := context.Background()
		schemaName := "public"
		tableName := "orders"
		fileName := "test_table"
		copyStmt := "COPY orders (id, user_id, order_amount, raise_error) FROM stdin;"
		entry := &toc.Entry{
			Namespace: &schemaName,
			Tag:       &tableName,
			FileName:  &fileName,
			CopyStmt:  &copyStmt,
		}
		data := "5\t1\t100.50\tTest exception\n"
		buf := new(bytes.Buffer)
		gzData := gzip.NewWriter(buf)
		_, err := gzData.Write([]byte(data))
		s.Require().NoError(err)
		err = gzData.Flush()
		s.Require().NoError(err)
		err = gzData.Close()
		s.Require().NoError(err)
		objSrc := &readCloserMock{Buffer: buf}

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, mock.Anything).Return(objSrc, nil)
		opt := &pgrestore.DataSectionSettings{
			ExitOnError:                      true,
			UseSessionReplicationRoleReplica: true,
			SuperUser:                        s.GetSuperUser(),
		}
		tr := NewTableRestorer(entry, st, opt)

		conn, err := s.GetConnectionWithUser(ctx, s.nonSuperUser, s.nonSuperUserPassword)
		s.Require().NoError(err)
		err = tr.Execute(ctx, utils.NewPGConn(conn))
		s.Require().NoError(err)
	})
}

func (s *restoresSuite) Test_TableRestorer_notices_during_copy() {
	// A trigger, a generated column or a domain check can make the server send
	// notices while COPY loads the rows. They must reach the connection's notice
	// handler instead of failing the restore.
	ctx := context.Background()
	setup, err := s.GetConnection(ctx)
	s.Require().NoError(err)
	defer setup.Close(ctx) // nolint: errcheck
	_, err = setup.Exec(ctx, `
CREATE TABLE copy_notices (id INT);
CREATE FUNCTION copy_notices_raise() RETURNS TRIGGER AS $$
BEGIN
	RAISE NOTICE 'notice for row %', NEW.id;
	RAISE WARNING 'warning for row %', NEW.id;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER copy_notices_raise BEFORE INSERT ON copy_notices
FOR EACH ROW EXECUTE FUNCTION copy_notices_raise();
`)
	s.Require().NoError(err)
	defer func() {
		_, err := setup.Exec(ctx, "DROP TABLE copy_notices; DROP FUNCTION copy_notices_raise();")
		s.NoError(err)
	}()

	for _, batchSize := range []int64{0, 2} {
		s.Run(fmt.Sprintf("batch size %d", batchSize), func() {
			_, err := setup.Exec(ctx, "TRUNCATE copy_notices")
			s.Require().NoError(err)

			schemaName := "public"
			tableName := "copy_notices"
			fileName := "test_table"
			copyStmt := "COPY copy_notices (id) FROM stdin;"
			entry := &toc.Entry{
				Namespace: &schemaName,
				Tag:       &tableName,
				FileName:  &fileName,
				CopyStmt:  &copyStmt,
			}
			buf := new(bytes.Buffer)
			gzData := gzip.NewWriter(buf)
			_, err = gzData.Write([]byte("1\n2\n3\n"))
			s.Require().NoError(err)
			s.Require().NoError(gzData.Flush())
			s.Require().NoError(gzData.Close())
			st := new(testutils.StorageMock)
			st.On("GetObject", ctx, mock.Anything).Return(&readCloserMock{Buffer: buf}, nil)
			opt := &pgrestore.DataSectionSettings{
				ExitOnError: true,
				BatchSize:   batchSize,
			}

			var notices []string
			cfg, err := pgx.ParseConfig(s.GetConnectionString(ctx))
			s.Require().NoError(err)
			cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
				notices = append(notices, n.Severity+": "+n.Message)
			}
			conn, err := pgx.ConnectConfig(ctx, cfg)
			s.Require().NoError(err)
			defer conn.Close(ctx) // nolint: errcheck

			err = NewTableRestorer(entry, st, opt).Execute(ctx, utils.NewPGConn(conn))
			s.Require().NoError(err)

			var rows int
			s.Require().NoError(setup.QueryRow(ctx, "SELECT count(*) FROM copy_notices").Scan(&rows))
			s.Equal(3, rows)
			s.Equal([]string{
				"NOTICE: notice for row 1", "WARNING: warning for row 1",
				"NOTICE: notice for row 2", "WARNING: warning for row 2",
				"NOTICE: notice for row 3", "WARNING: warning for row 3",
			}, notices)
		})
	}
}
