package restorers

import (
	"bytes"
	"compress/gzip"
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/utils/testutils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func (s *restoresSuite) Test_TableRestorerInsertFormat_check_triggers_errors() {
	s.Run("check triggers causes error by default", func() {
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
		data := "6\t1\t100.50\tTest exception\n"
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
		t := &toolkit.Table{
			Schema: schemaName,
			Name:   tableName,
			Columns: []*toolkit.Column{
				{
					Name:     "id",
					TypeName: "int4",
				},
				{
					Name:     "user_id",
					TypeName: "int4",
				},
				{
					Name:     "order_amount",
					TypeName: "numeric",
				},
				{
					Name:     "raise_error",
					TypeName: "text",
				},
			},
		}

		tr := NewTableRestorerInsertFormat(entry, t, st, opt, new(domains.DataRestorationErrorExclusions))

		conn, err := s.GetConnectionWithUser(ctx, s.nonSuperUser, s.nonSuperUserPassword)
		err = tr.Execute(ctx, conn)
		s.Require().ErrorContains(err, "Test exception (SQLSTATE P0001)")
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
		data := "7\t1\t100.50\tTest exception\n"
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
		t := &toolkit.Table{
			Schema: schemaName,
			Name:   tableName,
			Columns: []*toolkit.Column{
				{
					Name:     "id",
					TypeName: "int4",
				},
				{
					Name:     "user_id",
					TypeName: "int4",
				},
				{
					Name:     "order_amount",
					TypeName: "numeric",
				},
				{
					Name:     "raise_error",
					TypeName: "text",
				},
			},
		}

		tr := NewTableRestorerInsertFormat(entry, t, st, opt, new(domains.DataRestorationErrorExclusions))

		conn, err := s.GetConnectionWithUser(ctx, s.nonSuperUser, s.nonSuperUserPassword)
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
		data := "8\t1\t100.50\tTest exception\n"
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
		t := &toolkit.Table{
			Schema: schemaName,
			Name:   tableName,
			Columns: []*toolkit.Column{
				{
					Name:     "id",
					TypeName: "int4",
				},
				{
					Name:     "user_id",
					TypeName: "int4",
				},
				{
					Name:     "order_amount",
					TypeName: "numeric",
				},
				{
					Name:     "raise_error",
					TypeName: "text",
				},
			},
		}

		tr := NewTableRestorerInsertFormat(entry, t, st, opt, new(domains.DataRestorationErrorExclusions))

		conn, err := s.GetConnectionWithUser(ctx, s.nonSuperUser, s.nonSuperUserPassword)
		err = tr.Execute(ctx, conn)
		s.Require().NoError(err)
	})
}
