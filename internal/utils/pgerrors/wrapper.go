package pgerrors

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

type PgError struct {
	Err *pgproto3.ErrorResponse
}

func NewPgError(err *pgproto3.ErrorResponse) error {
	return &PgError{Err: err}
}

func (e *PgError) Error() string {
	return fmt.Sprintf("%s %s (code %s)", e.Err.Message, e.Err.Detail, e.Err.Code)
}
