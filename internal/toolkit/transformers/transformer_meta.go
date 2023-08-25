package transformers

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/data_section"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type TransformerFabricFunction func(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error)

type TransformerMeta struct {
	Description       string
	ParamsDescription map[string]string
	NewTransformer    TransformerFabricFunction
	Settings          *TransformerSettings
}

func (tm *TransformerMeta) InstanceTransformer(
	table *data_section.Table,
	typeMap *pgtype.Map,
	params map[string]interface{},
) (domains.Transformer, error) {
	if tm.Settings.TransformationType == domains.AttributeTransformation {
		base, err := NewTransformerBase(table, tm.Settings, params, typeMap, tm.Settings.CastVar)
		if err != nil {
			return nil, fmt.Errorf("cannot build transformer base: %w", err)
		}
		return tm.NewTransformer(base, params)
	}
	return nil, fmt.Errorf("unsupporterd transformer type")
}
