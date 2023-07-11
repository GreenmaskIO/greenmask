package domains

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

const (
	FatalErrorSeverity   = "fatal"
	WarningErrorSeverity = "warning"

	FkConstraintType         = "ForeignKey"
	CheckConstraintType      = "Check"
	NotNullConstraintType    = "Check"
	PkConstraintType         = "PrimaryKey"
	UniqueConstraintType     = "Unique"
	ReferencesConstraintType = "PrimaryKey"
	LengthConstraintType     = "Length"
	ExclusionConstraintType  = "Exclusion"
	TriggerConstraintType    = "TriggerConstraint"

	ConstraintObject = "Constraint"
)

type TransformerValidationErrors []error

func (ves TransformerValidationErrors) IsFatal() bool {
	return slices.ContainsFunc(ves, func(err error) bool {
		switch v := err.(type) {
		case *TransformerValidationError:
			return v.Severity == FatalErrorSeverity
		default:
			return true

		}
	})
}

type TransformerValidationError struct {
	ConstraintType   string `json:"constraintType,omitempty"`
	ConstraintName   string `json:"constraintName,omitempty"`
	ConstraintSchema string `json:"constraintSchema,omitempty"`
	ConstraintDef    string `json:"constraintDef,omitempty"`
	Severity         string `json:"severity,omitempty"`
	Err              error  `json:"err,omitempty"`
}

func (tve *TransformerValidationError) Error() string {
	return fmt.Sprintf("%s %s", tve.Severity, tve.Err)
}

func (tve *TransformerValidationError) SetLogEvent(event *zerolog.Event) *zerolog.Event {
	event = event.Str("ConstraintType", tve.ConstraintType)
	if tve.ConstraintType != "" {
		event.Str("ConstraintName", tve.ConstraintType)
	}
	if tve.ConstraintName != "" {
		event.Str("ConstraintName", tve.ConstraintName)
	}
	if tve.ConstraintSchema != "" {
		event.Str("ConstraintSchema", tve.ConstraintSchema)
	}
	if tve.ConstraintDef != "" {
		event.Str("ConstraintDef", tve.ConstraintDef)
	}
	if tve.Severity != "" {
		event.Str("Severity", tve.Severity)
	}
	if tve.Err != nil {
		event.Err(tve.Err)
	}
	return event
}

func (ves TransformerValidationErrors) LogErrors(schemaName, tableName, columnName, transformerName string) {
	for _, err := range ves {
		event := log.
			Warn().
			Str("SchemaName", schemaName).
			Str("TableName", tableName).
			Str("ColumnName", columnName).
			Str("TransformerName", transformerName)

		switch v := err.(type) {
		case *TransformerValidationError:
			event = v.SetLogEvent(event)
			event.Msgf("validation %s", v.Severity)
		default:
			log.Warn().Err(err).Msgf("internal error")
		}
	}
}

type Transformer interface {
	Transform(originalValue string) (string, error)
	Validate() TransformerValidationErrors
}

type TransformerConfig struct {
	Name   string                 `mapstructure:"name"`
	Params map[string]interface{} `mapstructure:"params"`
}
