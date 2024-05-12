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

package list_transformers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:   "list-transformers",
		Short: "list of the allowed transformers with documentation",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Err(err).Msg("")
			}

			if err := run(); err != nil {
				log.Fatal().Err(err).Msg("")
			}
		},
	}
	Config = domains.NewConfig()
	format string
)

const (
	JsonFormatName = "json"
	TextFormatName = "text"
)

const anyTypesValue = "any"

type parameter struct {
	Name           string   `json:"name,omitempty"`
	SupportedTypes []string `json:"supported_types,omitempty"`
}

type jsonResponse struct {
	Name        string       `json:"name,omitempty"`
	Description string       `json:"description,omitempty"`
	Parameters  []*parameter `json:"parameters,omitempty"`
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := custom.BootstrapCustomTransformers(ctx, utils.DefaultTransformerRegistry, Config.CustomTransformers)
	if err != nil {
		return fmt.Errorf("error registering custom transformer: %w", err)
	}

	// TODO: Consider about listing format. The transformer can have one and more columns as an input
	// 		and

	switch format {
	case JsonFormatName:
		err = listTransformersJson(utils.DefaultTransformerRegistry)
	case TextFormatName:
		err = listTransformersText(utils.DefaultTransformerRegistry)
	default:
		return fmt.Errorf(`unknown format %s`, format)
	}
	if err != nil {
		return fmt.Errorf("error listing transformers: %w", err)
	}

	return nil
}

func listTransformersJson(registry *utils.TransformerRegistry) error {
	var transformers []*jsonResponse

	for _, def := range registry.M {
		var params []*parameter
		for _, p := range def.Parameters {
			if !p.IsColumn && !p.IsColumnContainer {
				continue
			}
			supportedTypes := getColumnTypes(p)
			params = append(params, &parameter{Name: p.Name, SupportedTypes: supportedTypes})
		}

		transformers = append(transformers, &jsonResponse{
			Name:        def.Properties.Name,
			Description: def.Properties.Description,
			Parameters:  params,
		})
	}

	slices.SortFunc(transformers, func(a, b *jsonResponse) int {
		return strings.Compare(a.Name, b.Name)
	})

	if err := json.NewEncoder(os.Stdout).Encode(transformers); err != nil {
		return err
	}
	return nil
}

func listTransformersText(registry *utils.TransformerRegistry) error {

	var data [][]string
	table := tablewriter.NewWriter(os.Stdout)
	var names []string
	for name := range registry.M {
		names = append(names, name)
	}
	slices.Sort(names)
	table.SetHeader([]string{"name", "description", "column parameter name", "supported types"})
	for _, name := range names {
		def := registry.M[name]
		//allowedTypes := getAllowedTypesList(def)
		for _, p := range def.Parameters {
			if !p.IsColumn && !p.IsColumnContainer {
				continue
			}
			supportedTypes := getColumnTypes(p)
			data = append(data, []string{def.Properties.Name, def.Properties.Description, p.Name, strings.Join(supportedTypes, ", ")})
		}
	}

	table.AppendBulk(data)
	table.SetRowLine(true)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1})
	table.Render()

	return nil
}

func getColumnTypes(p *toolkit.ParameterDefinition) []string {
	if p.ColumnProperties != nil && len(p.ColumnProperties.AllowedTypes) > 0 {
		return p.ColumnProperties.AllowedTypes
	}
	return []string{anyTypesValue}
}

func init() {
	Cmd.Flags().StringVarP(&format, "format", "f", TextFormatName, "output format [text|json]")
}
