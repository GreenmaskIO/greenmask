package transformers

import (
	"github.com/google/uuid"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type UuidTransformer struct {
	Column pgDomains.ColumnMeta
}

func NewUuidTransformer(column pgDomains.ColumnMeta, params map[string]string) (domains.Transformer, error) {
	return &UuidTransformer{}, nil
}

func (rt *UuidTransformer) Transform(val string) (string, error) {
	return uuid.New().String(), nil
}
