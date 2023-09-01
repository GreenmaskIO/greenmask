package context

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/config"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	defaultTransformers "github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers2"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

func BuildTransformersMap() (map[string]*toolkit.Definition, error) {
	tm := make(map[string]*toolkit.Definition)
	for _, td := range defaultTransformers.DefaultTransformersList {
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
	dm map[string]*toolkit.Definition,
) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var totalWarnings toolkit.ValidationWarnings
	td, ok := dm[c.Name]
	if !ok {
		totalWarnings = append(totalWarnings,
			toolkit.NewValidationWarning().
				SetMsg("transformer not found").
				SetLevel(toolkit.ErrorValidationSeverity).SetTrace(&toolkit.Trace{
				SchemaName:      t.Schema,
				TableName:       t.Name,
				TransformerName: c.Name,
			}))
		return nil, totalWarnings, nil
	}
	driver, err := toolkit.NewDriver(tm, t.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("driver initialization for table %s.%s: %w", t.Schema, t.Name, err)
	}
	transformer, warnings, err := td.Instance(ctx, driver, c.Params, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to init transformer: %w", err)
	}
	return transformer, warnings, nil
}
