package restorers

import (
	"bytes"
	"compress/gzip"
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
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
		err = tr.Execute(ctx, conn)
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
		err = tr.Execute(ctx, conn)
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
		err = tr.Execute(ctx, conn)
		s.Require().NoError(err)
	})
}
