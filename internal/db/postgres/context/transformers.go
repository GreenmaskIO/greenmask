package context

import (
	"context"
	"fmt"

	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

func initTransformer(
	ctx context.Context, d *toolkit2.Driver,
	c *domains.TransformerConfig,
	r *transformersUtils.TransformerRegistry,
	types []*toolkit2.Type,
) (transformersUtils.Transformer, toolkit2.ValidationWarnings, error) {
	var totalWarnings toolkit2.ValidationWarnings
	td, ok := r.Get(c.Name)
	if !ok {
		totalWarnings = append(totalWarnings,
			toolkit2.NewValidationWarning().
				SetMsg("transformer not found").
				SetSeverity(toolkit2.ErrorValidationSeverity).SetTrace(&toolkit2.Trace{
				SchemaName:      d.Table.Schema,
				TableName:       d.Table.Name,
				TransformerName: c.Name,
			}))
		return nil, totalWarnings, nil
	}
	transformer, warnings, err := td.Instance(ctx, d, c.Params, types)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to init transformer: %w", err)
	}
	return transformer, warnings, nil
}
