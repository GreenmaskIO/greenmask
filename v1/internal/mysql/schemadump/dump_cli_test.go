package schemadump

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type stMock struct {
	mock.Mock
	gzData *bytes.Buffer
}

func (s *stMock) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	s.gzData = bytes.NewBuffer(nil)
	args := s.Called(ctx, filePath, body)
	_, err := io.Copy(s.gzData, body)
	if err != nil {
		return err
	}
	return args.Error(0)
}

type optsMock struct {
	mock.Mock
}

func (o *optsMock) SchemaDumpParams() ([]string, error) {
	args := o.Called()
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func TestDumpCli_Run(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		st := stMock{}
		expected := "CREATE TABLE test (id int);\n"
		st.On(
			"PutObject",
			mock.Anything, "schema.sql",
			mock.Anything,
		).Return(nil)
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return([]string{expected}, nil)
		d := NewDumpCli(&opts)
		d.executable = "echo"
		ctx := context.Background()
		err := d.Run(ctx, &st)
		require.NoError(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)

		gzReader, err := gzip.NewReader(st.gzData)
		require.NoError(t, err)
		actual, err := io.ReadAll(gzReader)
		require.NoError(t, err)
		require.Equal(t, expected+"\n", string(actual))
	})

	t.Run("error schema params", func(t *testing.T) {
		st := stMock{}
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return(nil, errors.New("some err"))
		d := NewDumpCli(&opts)
		ctx := context.Background()
		err := d.Run(ctx, &st)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 0)
	})

	t.Run("put object error", func(t *testing.T) {
		st := stMock{}
		st.On(
			"PutObject",
			mock.Anything, "schema.sql",
			mock.Anything,
		).Return(errors.New("put error"))
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return([]string{"CREATE TABLE test (id int);"}, nil)
		d := NewDumpCli(&opts)
		ctx := context.Background()
		err := d.Run(ctx, &st)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})

	t.Run("cmd error", func(t *testing.T) {
		st := stMock{}
		st.On(
			"PutObject",
			mock.Anything, "schema.sql",
			mock.Anything,
		).Return(nil)
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return([]string{"CREATE TABLE test (id int);"}, nil)
		d := NewDumpCli(&opts)
		d.executable = "121312 unknown command"
		ctx := context.Background()
		err := d.Run(ctx, &st)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})
}
