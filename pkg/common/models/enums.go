package models

import "fmt"

type DatabaseReplacementMode string

const (
	DatabaseReplaceModeStrict  DatabaseReplacementMode = "strict"
	DatabaseReplaceModeRelaxed DatabaseReplacementMode = "relaxed"
)

func (m DatabaseReplacementMode) Validate() error {
	switch m {
	case DatabaseReplaceModeRelaxed, DatabaseReplaceModeStrict:
		return nil
	default:
		return fmt.Errorf("value '%s': %w", m, ErrValueValidationFailed)
	}
}
