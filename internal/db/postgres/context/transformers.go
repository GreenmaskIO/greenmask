package context

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/config"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/dump"
	"github.com/GreenmaskIO/greenmask/pkg/toolkit/transformers"

	defaultTransformers "github.com/GreenmaskIO/greenmask/internal/db/postgres/transformers2"
)

func BuildTransformersMap() (map[string]*transformers.Definition, error) {
	tm := make(map[string]*transformers.Definition)
	for _, td := range defaultTransformers.TransformerRegistry {
		if _, ok := tm[td.Properties.Name]; ok {
			return nil, fmt.Errorf("transformer with name %s already exists", td.Properties.Name)
		}
		tm[td.Properties.Name] = td
	}
	return tm, nil
}

func initTransformer(
	ctx context.Context, t *dump.Table,
	c *config.TransformerConfig, tm *pgtype.Map,
	dm map[string]*transformers.Definition,
) (transformers.Transformer, transformers.ValidationWarnings, error) {
	var totalWarnings transformers.ValidationWarnings
	td, ok := dm[c.Name]
	if !ok {
		totalWarnings = append(totalWarnings,
			transformers.NewValidationWarning().
				SetMsg("transformer not found").
				SetLevel(transformers.ErrorValidationSeverity).SetTrace(&transformers.Trace{
				SchemaName:      t.Schema,
				TableName:       t.Name,
				TransformerName: c.Name,
			}))
		return nil, totalWarnings, nil
	}
	driver, err := transformers.NewDriver(tm, t.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("driver initialization for table %s.%s: %w", t.Schema, t.Name, err)
	}
	transformer, warnings, err := td.Instance(ctx, driver, c.Params, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to init transformer: %w", err)
	}
	return transformer, warnings, nil
}
