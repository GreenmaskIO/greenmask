package transformers

import (
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type TransformerFabricFunction func(column pgDomains.ColumnMeta, params map[string]string) (domains.Transformer, error)

type TransformerMeta struct {
	Description       string
	ParamsDescription map[string]string
	NewTransformer    TransformerFabricFunction
}

var (
	TransformerMap = map[string]TransformerMeta{
		"Replace": {
			Description:       `Replace with value passed through "value" parameter`,
			ParamsDescription: map[string]string{"value": "value that will be replaced instead of original"},
			NewTransformer:    NewReplaceTransformer,
		},
		"UUID": {
			Description:    `Generate random UUID`,
			NewTransformer: NewUuidTransformer,
		},
		"SetNull": {
			Description:    `Set NULL value`,
			NewTransformer: NewSetNullTransformer,
		},
		"GoTemplate": {
			Description:    "",
			NewTransformer: NewGoTemplateTransformer,
		},
	}
)
