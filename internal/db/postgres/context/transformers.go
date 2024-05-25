// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package context

import (
	"context"
	"fmt"

	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func initTransformer(
	ctx context.Context, d *toolkit.Driver,
	c *domains.TransformerConfig,
	r *transformersUtils.TransformerRegistry,
) (*transformersUtils.TransformerContext, toolkit.ValidationWarnings, error) {
	var totalWarnings toolkit.ValidationWarnings
	td, ok := r.Get(c.Name)
	if !ok {
		totalWarnings = append(totalWarnings,
			toolkit.NewValidationWarning().
				SetMsg("transformer not found").
				AddMeta("SchemaName", d.Table.Schema).
				AddMeta("TableName", d.Table.Name).
				AddMeta("TransformerName", c.Name).
				SetSeverity(toolkit.ErrorValidationSeverity),
		)
		return nil, totalWarnings, nil
	}
	transformer, warnings, err := td.Instance(ctx, d, c.Params, c.DynamicParams, c.When)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to init transformer: %w", err)
	}
	return transformer, warnings, nil
}
