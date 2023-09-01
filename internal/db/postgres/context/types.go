package context

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

func getCustomTypesUsedInTables(ctx context.Context, tx pgx.Tx) ([]*toolkit.Type, error) {
	return nil, errors.New("is not implemented")
}
