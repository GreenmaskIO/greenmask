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
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"

	"github.com/greenmaskio/greenmask/pkg/common/commands/listtransformers"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
)

// ForShowTransformer supplies the output format for the ShowTransformer operation.
func (g *Cli) ForShowTransformer(format OutputFormat) *Cli {
	g.showTransformerFormat = format
	return g
}

func (g *Cli) ShowTransformer(_ context.Context, name string) error {
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	if err := g.showTransformerFormat.Validate(); err != nil {
		return err
	}
	lister := listtransformers.New(registry.DefaultTransformerRegistry)
	item, ok := lister.Get(name)
	if !ok {
		return fmt.Errorf("unknown transformer %q", name)
	}
	switch g.showTransformerFormat {
	case OutputFormatJSON:
		return g.showTransformerJSON(item)
	case OutputFormatText:
		return g.showTransformerText(item)
	default:
		return fmt.Errorf("unknown format %s", g.showTransformerFormat)
	}
}

func (g *Cli) showTransformerJSON(item listtransformers.TransformerItem) error {
	if err := json.NewEncoder(os.Stdout).Encode(item); err != nil {
		return err
	}
	return nil
}

func (g *Cli) showTransformerText(item listtransformers.TransformerItem) error {
	table := tablewriter.NewWriter(os.Stdout)
	var data [][]string

	data = append(data, []string{
		item.Name, "description",
		item.Description, "",
		"", "",
	})
	for _, p := range item.Parameters {
		data = append(data, []string{
			item.Name, "parameters",
			p.Name, "description",
			p.Description, "",
		})
		data = append(data, []string{
			item.Name, "parameters",
			p.Name, "required",
			strconv.FormatBool(p.Required), "",
		})
		if p.DefaultValue != "" {
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "default",
				p.DefaultValue, "",
			})
		}
		if p.LinkColumnParameter != "" {
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "linked_parameter",
				p.LinkColumnParameter, "",
			})
		}
		if p.IsColumnContainer && p.ContainerProperties != nil {
			cp := p.ContainerProperties
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "column_properties",
				"types", strings.Join(cp.SupportedTypes, ", "),
			})
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "column_properties",
				"type_classes", strings.Join(cp.SupportedClasses, ", "),
			})
		}
		if p.IsColumn && p.ColumnProperties != nil {
			cp := p.ColumnProperties
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "column_properties",
				"types", strings.Join(cp.SupportedTypes, ", "),
			})
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "column_properties",
				"type_classes", strings.Join(cp.SupportedClasses, ", "),
			})
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "column_properties",
				"is_affected", strconv.FormatBool(cp.Affected),
			})
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "column_properties",
				"skip_original_data", strconv.FormatBool(cp.SkipOriginalData),
			})
			data = append(data, []string{
				item.Name, "parameters",
				p.Name, "column_properties",
				"skip_on_null", strconv.FormatBool(cp.SkipOnNull),
			})
		}
	}

	table.AppendBulk(data)
	table.SetRowLine(true)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1, 2, 3})
	table.Render()
	return nil
}
