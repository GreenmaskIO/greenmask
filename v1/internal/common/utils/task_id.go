package utils

import (
	"fmt"
	"strings"
)

// GetUniqueTaskID generates a unique task ID based on the task type and parts.
func GetUniqueTaskID(taskType string, parts ...string) string {
	objName := strings.Join(parts, ".")
	return fmt.Sprintf("%s___%s", taskType, objName)
}
