package processor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

type schemaDumperMock struct {
	mock.Mock
}

func (s *schemaDumperMock) DumpSchema(ctx context.Context) error {
	args := s.Called(ctx)
	return args.Error(0)
}

type dumpTaskMock struct {
	mock.Mock
}

func (d *dumpTaskMock) Dump(ctx context.Context) (commonmodels.DumpStat, error) {
	args := d.Called(ctx)
	if args.Error(1) != nil {
		return commonmodels.DumpStat{}, args.Error(1)
	}
	return args.Get(0).(commonmodels.DumpStat), args.Error(1)
}

func (d *dumpTaskMock) Meta() map[string]any {
	return make(map[string]any)
}

func (d *dumpTaskMock) DebugInfo() string {
	args := d.Called()
	return args.String(0)
}

type taskProducerMock struct {
	mock.Mock
}

func (t *taskProducerMock) Generate(
	ctx context.Context,
	vc *validationcollector.Collector,
) ([]commonininterfaces.Dumper, error) {
	args := t.Called(ctx, vc)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]commonininterfaces.Dumper), args.Error(1)
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

func TestProcessor_Run(t *testing.T) {
	t.Run("success", func(t *testing.T) {

		sd := &schemaDumperMock{}
		sd.On("DumpSchema").Return(nil)

		// Create 2 tasks.
		task1 := &dumpTaskMock{}
		task1.On("DebugInfo").
			Return("task1")
		task1.On("Dump", mock.Anything).
			Return(commonmodels.DumpStat{}, nil)
		task2 := &dumpTaskMock{}
		task2.On("Dump", mock.Anything).
			Return(commonmodels.DumpStat{}, nil)
		task2.On("DebugInfo").
			Return("task2")

		tp := &taskProducerMock{}
		// Produce the task list by the producer.
		tp.On("Generate", mock.Anything, mock.Anything).
			Return([]commonininterfaces.Dumper{task1, task2}, nil)

		sd.On("DumpSchema", mock.Anything).
			Return(nil)

		hbw := &heartBeatWorkerMock{}
		hbw.On("Run", mock.Anything, mock.Anything)

		vc := validationcollector.NewCollector()
		dumpRuntime := NewDefaultDumpProcessor(tp, hbw, sd)
		ctx := context.Background()
		err := dumpRuntime.Run(ctx, vc)
		require.NoError(t, err)

		task1.AssertExpectations(t)
		task2.AssertExpectations(t)
		tp.AssertExpectations(t)
		hbw.AssertExpectations(t)
		sd.AssertExpectations(t)
	})
}
