package faker

import (
	"github.com/jaswdr/faker"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type UuidTransformer struct {
	Column domains.ColumnMeta
	F      func() string
}

func (ut *UuidTransformer) Transform(originalValue string) (string, error) {
	return ut.F(), nil
}

func NewUuidTransformer(column domains.ColumnMeta, params map[string]string) (domains.Transformer, error) {
	return &UuidTransformer{
		Column: column,
		F:      faker.UUID{Faker: &faker.Faker{Generator: &RandomFakerGenerator{}}}.V4,
	}, nil
}
