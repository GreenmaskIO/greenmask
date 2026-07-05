package core

import (
	"context"
)

type SchemaDriftValidator interface {
	Compare(ctx context.Context, input SchemaDriftValidatorInput) SchemaDriftResult
}
