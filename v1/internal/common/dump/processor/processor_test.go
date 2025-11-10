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

func (d *dumpTaskMock) Dump(ctx context.Context) (commonmodels.TaskStat, error) {
	args := d.Called(ctx)
	if args.Error(1) != nil {
		return commonmodels.TaskStat{}, args.Error(1)
	}
	return args.Get(0).(commonmodels.TaskStat), args.Error(1)
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

func (t *taskProducerMock) Produce(
	ctx context.Context,
) ([]commonininterfaces.Dumper, commonmodels.RestorationContext, error) {
	args := t.Called(ctx)
	if args.Error(2) != nil {
		return nil, commonmodels.RestorationContext{}, args.Error(2)
	}
	return args.Get(0).([]commonininterfaces.Dumper), args.Get(1).(commonmodels.RestorationContext), args.Error(2)
}

func (t *taskProducerMock) Metadata(ctx context.Context) any {
	panic("implement me")
}

func TestProcessor_Run(t *testing.T) {
	t.Run("success", func(t *testing.T) {

		sd := &schemaDumperMock{}

		// Create 2 tasks.
		task1 := &dumpTaskMock{}
		task1.On("DebugInfo").
			Return("task1")
		task1.On("Dump", mock.Anything).
			Return(commonmodels.TaskStat{}, nil)
		task2 := &dumpTaskMock{}
		task2.On("Dump", mock.Anything).
			Return(commonmodels.TaskStat{}, nil)
		task2.On("DebugInfo").
			Return("task2")

		tp := &taskProducerMock{}
		// Produce the task list by the producer.
		tp.On("Produce", mock.Anything).
			Return([]commonininterfaces.Dumper{task1, task2}, commonmodels.RestorationContext{}, nil)

		sd.On("DumpSchema", mock.Anything).
			Return(nil)

		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		dumpRuntime, _ := NewDefaultDumpProcessor(tp, sd)
		_, err := dumpRuntime.Run(ctx)
		require.NoError(t, err)

		task1.AssertExpectations(t)
		task2.AssertExpectations(t)
		tp.AssertExpectations(t)
		sd.AssertExpectations(t)
	})
}
