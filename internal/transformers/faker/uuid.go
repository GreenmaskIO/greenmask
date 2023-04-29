package faker

import (
	"github.com/jaswdr/faker"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type UuidTransformer struct {
	Column pgDomains.ColumnMeta
	F      func() string
}

func (ut *UuidTransformer) Transform(originalValue string) (string, error) {
	return ut.F(), nil
}

func NewUuidTransformer(column pgDomains.ColumnMeta, params map[string]string) (domains.Transformer, error) {
	return &UuidTransformer{
		Column: column,
		F:      faker.UUID{Faker: &faker.Faker{Generator: &RandomFakerGenerator{}}}.V4,
	}, nil
}
