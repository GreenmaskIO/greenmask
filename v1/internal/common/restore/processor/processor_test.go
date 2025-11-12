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
	tasks   []commonininterfaces.Restorer
	current int
}

func newTaskProducerMock(tasks []commonininterfaces.Restorer) *taskProducerMock {
	return &taskProducerMock{
		tasks:   tasks,
		current: -1,
	}
}

func (t *taskProducerMock) Next(ctx context.Context) bool {
	t.current++
	if t.current >= len(t.tasks) || len(t.tasks) == 0 {
		return false
	}
	t.Called(ctx)
	return true
}

func (t *taskProducerMock) Err() error {
	args := t.Called()
	return args.Error(0)
}

func (t *taskProducerMock) Task() (commonininterfaces.Restorer, error) {
	t.Called()
	return t.tasks[t.current], nil
}

func TestProcessor_Run(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()

		sr := &schemaRestorerMock{}
		sr.On("RestoreSchema", mock.Anything).
			Return(nil)

		// Create 2 tasks.
		task1 := &restoreTaskMock{}
		task1.On("Init", mock.Anything).
			Return(nil)
		task1.On("DebugInfo").
			Return("task1")
		task1.On("Restore", mock.Anything).
			Return(nil)
		task1.On("Close", mock.Anything).
			Return(nil)

		task2 := &restoreTaskMock{}
		task2.On("Init", mock.Anything).
			Return(nil)
		task2.On("Restore", mock.Anything).
			Return(nil)
		task2.On("DebugInfo").
			Return("task2")
		task2.On("Close", mock.Anything).
			Return(nil)

		tp := newTaskProducerMock([]commonininterfaces.Restorer{task1, task2})
		//Produce the task list by the producer.
		tp.On("Next", mock.Anything)
		tp.On("Err").Return(nil)
		tp.On("Task")

		cfg := Config{
			Jobs:           2,
			RestoreInOrder: true,
		}

		dumpRuntime := NewDefaultRestoreProcessor(ctx, tp, sr, cfg)
		err := dumpRuntime.Run(ctx)
		require.NoError(t, err)

		task1.AssertExpectations(t)
		task2.AssertExpectations(t)
		tp.AssertExpectations(t)
	})
}
