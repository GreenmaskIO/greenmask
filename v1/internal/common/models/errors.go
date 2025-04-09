package models

import "errors"

var (
	ErrFatalValidationError  = errors.New("fatal validation error")
	ErrCanonicalTypeMismatch = errors.New("canonical type mismatch")
)
