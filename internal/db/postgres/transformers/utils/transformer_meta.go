package utils

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/toclib"

	"github.com/GreenmaskIO/greenmask/internal/domains"
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
	table *toclib.Table,
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
