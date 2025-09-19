package interfaces

import commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"

type TaskMapper interface {
	IsTaskCompleted(taskID commonmodels.TaskID) bool
	SetTaskCompleted(taskID commonmodels.TaskID)
}
