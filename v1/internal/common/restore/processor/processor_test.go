// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

func (d *restoreTaskMock) Init(ctx context.Context) error {
	args := d.Called(ctx)
	return args.Error(0)
}

func (d *restoreTaskMock) Close(ctx context.Context) error {
	args := d.Called(ctx)
	return args.Error(0)
}

type taskProducerMock struct {
	mock.Mock
}

func (t *taskProducerMock) Next(ctx context.Context) bool {
	//TODO implement me
	panic("implement me")
}

func (t *taskProducerMock) Err() error {
	//TODO implement me
	panic("implement me")
}

func (t *taskProducerMock) Task() (commonininterfaces.Restorer, error) {
	//TODO implement me
	panic("implement me")
}

func (t *taskProducerMock) Produce(
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
		tp.On("Produce", mock.Anything, mock.Anything).
			Return([]commonininterfaces.Restorer{task1, task2}, nil)

		sr.On("RestoreSchema", mock.Anything).
			Return(nil)

		dumpRuntime := NewDefaultRestoreProcessor(tp, sr)
		ctx := context.Background()
		err := dumpRuntime.Run(ctx)
		require.NoError(t, err)

		task1.AssertExpectations(t)
		task2.AssertExpectations(t)
		tp.AssertExpectations(t)
	})
}
