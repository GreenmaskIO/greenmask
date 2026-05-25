package models

import "fmt"

type DumpValidationReport struct {
	Warnings []*ValidationWarning
}

func (r DumpValidationReport) HasErrors() bool {
	for _, w := range r.Warnings {
		if w.Severity == ValidationSeverityError {
			return true
		}
	}
	return false
}

func (r DumpValidationReport) AsError() error {
	var msg string
	for _, w := range r.Warnings {
		if w.Severity == ValidationSeverityError {
			msg += fmt.Sprintf("%s; ", w.Msg)
		}
	}
	if msg == "" {
		return nil
	}
	return fmt.Errorf("%s", msg)
}

type DumpPlanValidationInput struct {
	Plan DumpPlan
}
