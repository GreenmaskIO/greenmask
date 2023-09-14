package context

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func initTransformer(
	ctx context.Context, t *dump.Table,
	c *domains.TransformerConfig, tm *pgtype.Map,
	r *transformersUtils.TransformerRegistry,
) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var totalWarnings toolkit.ValidationWarnings
	td, ok := r.Get(c.Name)
	if !ok {
		totalWarnings = append(totalWarnings,
			toolkit.NewValidationWarning().
				SetMsg("transformer not found").
				SetSeverity(toolkit.ErrorValidationSeverity).SetTrace(&toolkit.Trace{
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
