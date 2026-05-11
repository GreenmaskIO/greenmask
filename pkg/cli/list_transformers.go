// Copyright 2025 Greenmask
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

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"

	"github.com/greenmaskio/greenmask/pkg/common/listtransformers"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
)

// ForListTransformers supplies the output format for the ListTransformers operation.
func (g *Cli) ForListTransformers(format OutputFormat) *Cli {
	g.listTransformersFormat = format
	return g
}

func (g *Cli) ListTransformers(_ context.Context) error {
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	if err := g.listTransformersFormat.Validate(); err != nil {
		return err
	}
	lister := listtransformers.New(registry.DefaultTransformerRegistry)
	items := lister.List()
	switch g.listTransformersFormat {
	case OutputFormatJSON:
		return g.printTransformersJSON(items)
	case OutputFormatText:
		return g.printTransformersText(items)
	default:
		return fmt.Errorf("unknown format %s", g.listTransformersFormat)
	}
}

func (g *Cli) printTransformersJSON(items []listtransformers.TransformerItem) error {
	if err := json.NewEncoder(os.Stdout).Encode(items); err != nil {
		return err
	}
	return nil
}

func (g *Cli) printTransformersText(items []listtransformers.TransformerItem) error {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{
		"name",
		"description",
		"column parameter name",
		"supported types",
		"supported type classes",
	})
	var data [][]string
	for _, item := range items {
		var colParams []listtransformers.ParameterItem
		for _, p := range item.Parameters {
			if p.IsColumn || p.IsColumnContainer {
				colParams = append(colParams, p)
			}
		}
		if len(colParams) > 0 {
			for _, p := range colParams {
				var types, classes []string
				if p.ColumnProperties != nil {
					types = p.ColumnProperties.SupportedTypes
					classes = p.ColumnProperties.SupportedClasses
				} else if p.ContainerProperties != nil {
					types = p.ContainerProperties.SupportedTypes
					classes = p.ContainerProperties.SupportedClasses
				}
				data = append(data, []string{
					item.Name,
					item.Description,
					p.Name,
					strings.Join(types, ", "),
					strings.Join(classes, ", "),
				})
			}
		} else {
			data = append(data, []string{
				item.Name,
				item.Description,
				"", "", "",
			})
		}
	}
	table.AppendBulk(data)
	table.SetRowLine(true)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1})
	table.Render()
	return nil
}
