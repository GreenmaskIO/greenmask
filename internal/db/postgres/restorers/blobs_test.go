package restorers

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/utils"
	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
	"github.com/greenmaskio/greenmask/internal/utils/testutils"
)

func (s *restoresSuite) Test_BlobsRestorer_getBlobsOIds() {
	s.Run("basic", func() {
		ctx := context.Background()

		objSrc := newReadCloserMock()
		objSrc.Write([]byte(`26176 blob_26176.dat
26177 blob_26177.dat
`))

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, mock.Anything).Return(objSrc, nil)

		br := NewBlobsRestorer(&toc.Entry{}, st, false)
		loOids, err := br.getBlobsOids(ctx)
		s.Require().NoError(err)
		s.Require().Len(loOids, 2)
		s.Require().ElementsMatch([]uint32{26176, 26177}, loOids)
	})

	s.Run("error get object", func() {
		ctx := context.Background()

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, mock.Anything).
			Return(nil, errors.New("test err"))

		br := NewBlobsRestorer(&toc.Entry{}, st, false)
		_, err := br.getBlobsOids(ctx)
		s.Require().Error(err)
	})

	s.Run("error parsing oid", func() {
		ctx := context.Background()

		objSrc := newReadCloserMock()
		objSrc.Write([]byte(`2asad blob_26176.dat
26177 blob_26177.dat
`))

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, mock.Anything).Return(objSrc, nil)

		br := NewBlobsRestorer(&toc.Entry{}, st, false)
		_, err := br.getBlobsOids(ctx)
		s.Require().ErrorContains(err, "parse oid")
	})
}

func (s *restoresSuite) Test_BlobsRestorer_getLargeObjectDataReader() {
	s.Run("basic", func() {
		original := "abc123"
		buf := new(bytes.Buffer)
		gzData := gzip.NewWriter(buf)
		_, err := gzData.Write([]byte(original))
		s.Require().NoError(err)
		err = gzData.Flush()
		s.Require().NoError(err)
		err = gzData.Close()
		s.Require().NoError(err)
		objSrc := &readCloserMock{Buffer: buf}

		ctx := context.Background()

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, "blob_123.dat.gz").
			Return(objSrc, nil)

		br := NewBlobsRestorer(&toc.Entry{}, st, false)
		r, err := br.getLargeObjectDataReader(ctx, 123)
		s.Require().NoError(err)
		s.Require().NotNil(r)
		expected := "abc123"
		actual, err := io.ReadAll(r)
		s.Require().NoError(err)
		s.Require().Equal(expected, string(actual))
		st.AssertNumberOfCalls(s.T(), "GetObject", 1)
	})

	s.Run("error get object", func() {
		ctx := context.Background()

		st := new(testutils.StorageMock)
		st.On("GetObject", ctx, "blob_123.dat.gz").
			Return(nil, errors.New("test err"))

		br := NewBlobsRestorer(&toc.Entry{}, st, false)
		_, err := br.getLargeObjectDataReader(ctx, 123)
		s.Require().Error(err)
		st.AssertNumberOfCalls(s.T(), "GetObject", 1)
	})
}

func (s *restoresSuite) Test_BlobsRestorer_restoreLargeObjectData() {

	s.Run("basic", func() {
		original := "abc123"
		buf := new(bytes.Buffer)
		gzData := gzip.NewWriter(buf)
		_, err := gzData.Write([]byte(original))
		s.Require().NoError(err)
		err = gzData.Flush()
		s.Require().NoError(err)
		err = gzData.Close()
		s.Require().NoError(err)
		objSrc := &readCloserMock{Buffer: buf}
		gz, err := ioutils.GetGzipReadCloser(objSrc, false)
		s.Require().NoError(err)

		ctx := context.Background()
		// Create lo
		conn, err := s.GetConnection(ctx)
		s.Require().NoError(err)
		var loOid uint32
		err = conn.QueryRow(ctx, "SELECT lo_create(0)").Scan(&loOid)
		s.Require().NoError(err)
		st := new(testutils.StorageMock)

		br := NewBlobsRestorer(&toc.Entry{}, st, false)
		tx, err := conn.Begin(ctx)
		s.Require().NoError(err)
		defer tx.Rollback(ctx) // nolint: errcheck
		err = br.restoreLargeObjectData(ctx, tx, gz, loOid)
		s.Require().NoError(err)
		err = tx.Commit(ctx)
		s.Require().NoError(err)

		tx, err = conn.Begin(ctx)
		s.Require().NoError(err)
		loAPI := tx.LargeObjects()
		lo, err := loAPI.Open(ctx, loOid, pgx.LargeObjectModeRead)
		s.Require().NoError(err)
		defer lo.Close()
		actual, err := io.ReadAll(lo)
		s.Require().NoError(err)
		expected := "abc123"
		s.Require().Equal(expected, string(actual))
	})
}

func (s *restoresSuite) Test_BlobsRestorer_Execute() {
	s.Run("basic", func() {
		ctx := context.Background()
		// Create lo
		conn, err := s.GetConnection(ctx)
		s.Require().NoError(err)

		var loOid1 uint32
		err = conn.QueryRow(ctx, "SELECT lo_create(0)").Scan(&loOid1)
		s.Require().NoError(err)
		var loOid2 uint32
		err = conn.QueryRow(ctx, "SELECT lo_create(0)").Scan(&loOid2)
		s.Require().NoError(err)

		blobsTocData := newReadCloserMock()
		blobsTocData.Write([]byte(fmt.Sprintf(
			`%d blob_%d.dat
%d blob_%d.dat
`, loOid1, loOid1, loOid2, loOid2)))

		obj1 := newReadCloserMock()
		gzw1 := gzip.NewWriter(obj1)
		_, err = gzw1.Write([]byte("obj1"))
		s.Require().NoError(err)
		err = gzw1.Flush()
		s.Require().NoError(err)
		err = gzw1.Close()
		s.Require().NoError(err)

		obj2 := newReadCloserMock()
		gzw2 := gzip.NewWriter(obj2)
		_, err = gzw2.Write([]byte("obj2"))
		s.Require().NoError(err)
		err = gzw2.Flush()
		s.Require().NoError(err)
		err = gzw2.Close()
		s.Require().NoError(err)
		s.Require().NoError(err)

		st := new(testutils.StorageMock)
		st.On("GetObject", mock.Anything, "blobs.toc").Return(blobsTocData, nil)
		st.On("GetObject", mock.Anything, fmt.Sprintf("blob_%d.dat.gz", loOid1)).Return(obj1, nil)
		st.On("GetObject", mock.Anything, fmt.Sprintf("blob_%d.dat.gz", loOid2)).Return(obj2, nil)

		br := NewBlobsRestorer(&toc.Entry{}, st, false)
		err = br.Execute(ctx, utils.NewPGConn(conn))
		s.Require().NoError(err)

		// validate LO
		for i, oid := range []uint32{loOid1, loOid2} {
			s.Run(fmt.Sprintf("validate LO %d", oid), func() {
				tx, err := conn.Begin(ctx)
				s.Require().NoError(err)
				defer tx.Rollback(ctx) // nolint: errcheck
				loAPI := tx.LargeObjects()
				lo, err := loAPI.Open(ctx, oid, pgx.LargeObjectModeRead)
				s.Require().NoError(err)
				defer lo.Close()
				actual, err := io.ReadAll(lo)
				s.Require().NoError(err)
				expected := fmt.Sprintf("obj%d", i+1)
				s.Require().Equal(expected, string(actual))
			})
		}

	})
}
