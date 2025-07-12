package datadump

import (
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

type storageMock struct {
	mock.Mock
}

func (s *storageMock) GetCwd() string {
	args := s.Called()
	return args.String(0)
}

func (s *storageMock) Dirname() string {
	args := s.Called()
	return args.String(0)
}

func (s *storageMock) ListDir(ctx context.Context) (files []string, dirs []storages.Storager, err error) {
	args := s.Called(ctx)
	if args.Error(2) != nil {
		return nil, nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Get(1).([]storages.Storager), nil
}

func (s *storageMock) GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	args := s.Called(ctx, filePath)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(io.ReadCloser), nil
}

func (s *storageMock) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	args := s.Called(ctx, filePath, body)
	if args.Error(1) != nil {
		return args.Error(1)
	}
	return nil
}

func (s *storageMock) Delete(ctx context.Context, filePaths ...string) error {
	args := s.Called(ctx, filePaths)
	if args.Error(1) != nil {
		return args.Error(1)
	}
	return nil
}

func (s *storageMock) DeleteAll(ctx context.Context, pathPrefix string) error {
	args := s.Called(ctx, pathPrefix)
	if args.Error(1) != nil {
		return args.Error(1)
	}
	return nil
}

func (s *storageMock) Exists(ctx context.Context, fileName string) (bool, error) {
	args := s.Called(ctx, fileName)
	if args.Error(1) != nil {
		return false, args.Error(1)
	}
	return args.Bool(0), nil
}

func (s *storageMock) SubStorage(subPath string, relative bool) storages.Storager {
	args := s.Called(subPath, relative)
	return args.Get(0).(storages.Storager)
}

func (s *storageMock) Stat(fileName string) (*storages.ObjectStat, error) {
	args := s.Called(fileName)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storages.ObjectStat), nil
}

type dumpTaskMock struct {
	mock.Mock
}

func (d *dumpTaskMock) Dump(ctx context.Context, st storages.Storager) error {
	args := d.Called(ctx, st)
	return args.Error(0)
}

func (d *dumpTaskMock) DebugInfo() string {
	args := d.Called()
	return args.String(0)
}

type taskProducerMock struct {
	mock.Mock
}

func (t *taskProducerMock) Produce(ctx context.Context) ([]dumpTask, error) {
	args := t.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]dumpTask), nil
}

func (t *taskProducerMock) Metadata(ctx context.Context) any {
	panic("implement me")
}

type heartBeatWorkerMock struct {
	mock.Mock
}

func (h *heartBeatWorkerMock) Run(ctx context.Context, done <-chan struct{}) func() error {
	_ = h.Called(ctx, done)
	return func() error {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func TestDumpRuntime_Run(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		st := &storageMock{}
		st.On("SubStorage", mock.Anything, true).
			Run(func(args mock.Arguments) {
				prefix := args.Get(0).(string)
				_, err := strconv.Atoi(prefix)
				assert.NoErrorf(t, err, "prefix should be an integer")
			}).
			Return(st)

		// Create 2 tasks.
		task1 := &dumpTaskMock{}
		task1.On("DebugInfo").
			Return("task1")
		task1.On("Dump", mock.Anything, st).
			Return(nil)
		task2 := &dumpTaskMock{}
		task2.On("Dump", mock.Anything, st).
			Return(nil)
		task2.On("DebugInfo").
			Return("task2")

		tp := &taskProducerMock{}
		// Produce the task list by the producer.
		tp.On("Build", mock.Anything).
			Return([]dumpTask{task1, task2}, nil)

		hbw := &heartBeatWorkerMock{}
		hbw.On("Run", mock.Anything, mock.Anything)

		dumpRuntime := NewDefaultDataDumper(tp, hbw, st)
		ctx := context.Background()
		err := dumpRuntime.Run(ctx)
		require.NoError(t, err)
	})
}
