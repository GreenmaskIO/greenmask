package processor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

type schemaRestorerMock struct {
	mock.Mock
}

func (s *schemaRestorerMock) RestoreSchema(ctx context.Context) error {
	args := s.Called(ctx)
	return args.Error(0)
}

type restoreTaskMock struct {
	mock.Mock
}

func (d *restoreTaskMock) Restore(ctx context.Context) error {
	args := d.Called(ctx)
	return args.Error(0)
}

func (d *restoreTaskMock) Meta() map[string]any {
	return make(map[string]any)
}

func (d *restoreTaskMock) DebugInfo() string {
	args := d.Called()
	return args.String(0)
}

type taskProducerMock struct {
	mock.Mock
}

func (t *taskProducerMock) Generate(
	ctx context.Context,
	vc *validationcollector.Collector,
) ([]commonininterfaces.Restorer, error) {
	args := t.Called(ctx, vc)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]commonininterfaces.Restorer), args.Error(1)
}

func TestProcessor_Run(t *testing.T) {
	t.Run("success", func(t *testing.T) {

		sr := &schemaRestorerMock{}
		sr.On("RestoreSchema").Return(nil)

		// Create 2 tasks.
		task1 := &restoreTaskMock{}
		task1.On("DebugInfo").
			Return("task1")
		task1.On("Restore", mock.Anything).
			Return(nil)
		task2 := &restoreTaskMock{}
		task2.On("Restore", mock.Anything).
			Return(nil)
		task2.On("DebugInfo").
			Return("task2")

		tp := &taskProducerMock{}
		// Produce the task list by the producer.
		tp.On("Generate", mock.Anything, mock.Anything).
			Return([]commonininterfaces.Restorer{task1, task2}, nil)

		sr.On("RestoreSchema", mock.Anything).
			Return(nil)

		vc := validationcollector.NewCollector()
		dumpRuntime := NewDefaultRestoreProcessor(tp, sr)
		ctx := context.Background()
		err := dumpRuntime.Run(ctx, vc)
		require.NoError(t, err)

		task1.AssertExpectations(t)
		task2.AssertExpectations(t)
		tp.AssertExpectations(t)
	})
}
