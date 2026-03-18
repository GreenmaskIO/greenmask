package cmdrun

import (
	"fmt"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type OutputFormat string

const (
	OutputFormatJSON OutputFormat = "json"
	OutputFormatText OutputFormat = "text"
)

func (of OutputFormat) Validate() error {
	switch of {
	case OutputFormatJSON, OutputFormatText:
		return nil
	default:
		return fmt.Errorf("format '%s': %w", of, commonmodels.ErrValueValidationFailed)
	}
}
