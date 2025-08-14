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

package show_transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

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
		Use:   "show-transformer [name]",
		Args:  cobra.MaximumNArgs(1),
		Short: "show transformer details or list all transformers if no name provided",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Err(err).Msg("")
			}

			transformerName := ""
			if len(args) > 0 {
				transformerName = args[0]
			}

			if err := run(transformerName); err != nil {
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

func run(name string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := custom.BootstrapCustomTransformers(ctx, utils.DefaultTransformerRegistry, Config.CustomTransformers)
	if err != nil {
		return fmt.Errorf("error registering custom transformer: %w", err)
	}

	// If no name is provided, list all transformers
	if name == "" {
		switch format {
		case JsonFormatName:
			err = listAllTransformersJson(utils.DefaultTransformerRegistry)
		case TextFormatName:
			err = listAllTransformersText(utils.DefaultTransformerRegistry)
		default:
			return fmt.Errorf(`unknown format \"%s\"`, format)
		}
	} else {
		switch format {
		case JsonFormatName:
			err = showTransformerJson(utils.DefaultTransformerRegistry, name)
		case TextFormatName:
			err = showTransformerText(utils.DefaultTransformerRegistry, name)
		default:
			return fmt.Errorf(`unknown format \"%s\"`, format)
		}
	}
	if err != nil {
		return fmt.Errorf("error listing transformers: %w", err)
	}

	return nil
}

func showTransformerJson(registry *utils.TransformerRegistry, transformerName string) error {
	var transformers []*utils.TransformerDefinition

	def, ok := registry.M[transformerName]
	if ok {
		transformers = append(transformers, def)
	} else {
		return fmt.Errorf("unknown transformer with name \"%s\"", transformerName)
	}

	if err := json.NewEncoder(os.Stdout).Encode(transformers); err != nil {
		return err
	}
	return nil
}

func showTransformerText(registry *utils.TransformerRegistry, name string) error {

	var data [][]string
	table := tablewriter.NewWriter(os.Stdout)

	def, err := getTransformerDefinition(registry, name)
	if err != nil {
		return err
	}

	data = append(data, []string{def.Properties.Name, "description", def.Properties.Description, "", "", ""})
	for _, p := range def.Parameters {
		data = append(data, []string{def.Properties.Name, "parameters", p.Name, "description", p.Description, ""})
		data = append(data, []string{def.Properties.Name, "parameters", p.Name, "required", strconv.FormatBool(p.Required), ""})
		if p.DefaultValue != nil {
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "default", string(p.DefaultValue), ""})
		}
		if p.LinkColumnParameter != "" {
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "linked_parameter", p.LinkColumnParameter, ""})
		}
		if p.CastDbType != "" {
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "cast_to_db_type", p.CastDbType, ""})
		}
		if p.IsColumnContainer {
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "allowed_types", anyTypesValue})
		}
		if p.ColumnProperties != nil {
			allowedTypes := []string{anyTypesValue}
			if len(p.ColumnProperties.AllowedTypes) > 0 {
				allowedTypes = p.ColumnProperties.AllowedTypes
			}
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "allowed_types", strings.Join(allowedTypes, ", ")})
			isAffected := strconv.FormatBool(p.ColumnProperties.Affected)
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "is_affected", isAffected})
			skipOriginalData := strconv.FormatBool(p.ColumnProperties.SkipOriginalData)
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "skip_original_data", skipOriginalData})
			skipOnNull := strconv.FormatBool(p.ColumnProperties.SkipOnNull)
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "skip_on_null", skipOnNull})
		}
	}

	table.AppendBulk(data)
	table.SetRowLine(true)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1, 2, 3})
	table.Render()

	return nil
}

func getTransformerDefinition(registry *utils.TransformerRegistry, name string) (*utils.TransformerDefinition, error) {
	def, ok := registry.M[name]
	if ok {
		return def, nil
	}
	return nil, fmt.Errorf("unknown transformer \"%s\"", name)
}

func listAllTransformersJson(registry *utils.TransformerRegistry) error {
	var transformers []*utils.TransformerDefinition

	// Add all transformers to the list
	for _, def := range registry.M {
		transformers = append(transformers, def)
	}

	// Sort transformers by name
	sort.Slice(transformers, func(i, j int) bool {
		return transformers[i].Properties.Name < transformers[j].Properties.Name
	})

	if err := json.NewEncoder(os.Stdout).Encode(transformers); err != nil {
		return err
	}
	return nil
}

func listAllTransformersText(registry *utils.TransformerRegistry) error {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Description"})

	var rows [][]string
	for _, def := range registry.M {
		rows = append(rows, []string{
			def.Properties.Name,
			def.Properties.Description,
		})
	}

	// Sort rows by transformer name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0] < rows[j][0]
	})

	fmt.Printf("Found %d transformers:\n", len(registry.M))

	table.AppendBulk(rows)
	table.Render()

	fmt.Println("Use 'show-transformer <name>' to see detailed information about a specific transformer")
	return nil
}

func init() {
	Cmd.Flags().StringVarP(&format, "format", "f", TextFormatName, "output format [text|json]")
}
