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
