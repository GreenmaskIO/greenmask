package schema

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
)

type optsMock struct {
	mock.Mock
}

func (o *optsMock) Env() ([]string, error) {
	args := o.Called()
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
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
		st := mocks.NewStorageMock()
		expected := "CREATE TABLE test (id int);\n"
		st.On(
			"PutObject",
			mock.Anything, "schema.sql",
			mock.Anything,
		).Return(nil)
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return([]string{expected}, nil)
		opts.On("Env").Return([]string{"_TEST=1"}, nil)
		d := New(st, &opts)
		d.executable = "echo"
		ctx := context.Background()
		err := d.DumpSchema(ctx)
		require.NoError(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)

		actual := st.Data.String()
		require.NoError(t, err)
		require.Equal(t, expected+"\n", actual)
	})

	t.Run("error schema params", func(t *testing.T) {
		st := mocks.NewStorageMock()
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return(nil, errors.New("some err"))
		opts.On("Env").Return([]string{"_TEST=1"}, nil)
		d := New(st, &opts)
		ctx := context.Background()
		err := d.DumpSchema(ctx)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 0)
	})

	t.Run("put object error", func(t *testing.T) {
		st := mocks.NewStorageMock()
		st.On(
			"PutObject",
			mock.Anything, "schema.sql",
			mock.Anything,
		).Return(errors.New("put error"))
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return([]string{"CREATE TABLE test (id int);"}, nil)
		opts.On("Env").Return([]string{"_TEST=1"}, nil)
		d := New(st, &opts)
		ctx := context.Background()
		err := d.DumpSchema(ctx)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})

	t.Run("cmd error", func(t *testing.T) {
		st := mocks.NewStorageMock()
		st.On(
			"PutObject",
			mock.Anything, "schema.sql",
			mock.Anything,
		).Return(nil)
		opts := optsMock{}
		opts.On("SchemaDumpParams").Return([]string{"CREATE TABLE test (id int);"}, nil)
		opts.On("Env").Return([]string{"_TEST=1"}, nil)
		d := New(st, &opts)
		d.executable = "121312 unknown command"
		ctx := context.Background()
		err := d.DumpSchema(ctx)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})
}
