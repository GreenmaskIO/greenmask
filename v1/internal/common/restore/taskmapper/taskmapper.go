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

package taskmapper

import (
	"sync"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type TaskResolver struct {
	completedTasks map[commonmodels.TaskID]struct{}
	mx             sync.RWMutex
}

func NewTaskResolver() *TaskResolver {
	return &TaskResolver{
		completedTasks: make(map[commonmodels.TaskID]struct{}),
	}
}

func (m *TaskResolver) IsTaskCompleted(taskID commonmodels.TaskID) bool {
	m.mx.RLock()
	defer m.mx.RUnlock()
	_, ok := m.completedTasks[taskID]
	return ok
}

func (m *TaskResolver) SetTaskCompleted(taskID commonmodels.TaskID) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.completedTasks[taskID] = struct{}{}
}
