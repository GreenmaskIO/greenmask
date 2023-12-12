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
		Use:   "list-transformers",
		Short: "list of the allowed transformers with documentation",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Err(err).Msg("")
			}

			if err := run(args); err != nil {
				log.Err(err).Msg("")
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

func run(transformerNames []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := custom.BootstrapCustomTransformers(ctx, utils.DefaultTransformerRegistry, Config.CustomTransformers)
	if err != nil {
		return fmt.Errorf("error registering custom transformer: %w", err)
	}

	switch format {
	case JsonFormatName:
		err = listTransformersJson(utils.DefaultTransformerRegistry, transformerNames)
	case TextFormatName:
		err = listTransformersText(utils.DefaultTransformerRegistry, transformerNames)
	default:
		return fmt.Errorf(`unknown format %s`, format)
	}
	if err != nil {
		return fmt.Errorf("error listing transformers: %w", err)
	}

	return nil
}

func listTransformersJson(registry *utils.TransformerRegistry, transformerNames []string) error {
	var transformers []*utils.Definition

	if len(transformerNames) > 0 {

		for _, name := range transformerNames {
			def, ok := registry.M[name]
			if ok {
				transformers = append(transformers, def)
			} else {
				return fmt.Errorf("unknown transformer name \"%s\"", name)
			}
		}

	} else {
		for _, def := range registry.M {
			transformers = append(transformers, def)
		}
	}

	if err := json.NewEncoder(os.Stdout).Encode(transformers); err != nil {
		return err
	}
	return nil
}

func listTransformersText(registry *utils.TransformerRegistry, transformerNames []string) error {

	var data [][]string
	table := tablewriter.NewWriter(os.Stdout)
	var names []string
	if len(transformerNames) > 0 {
		for _, name := range transformerNames {
			_, ok := registry.M[name]
			if ok {
				names = append(names, name)
			} else {
				return fmt.Errorf("unknown transformer name \"%s\"", name)
			}
		}

	} else {
		for name := range registry.M {
			names = append(names, name)
		}
		slices.Sort(names)
	}

	for _, name := range names {
		def := registry.M[name]
		data = append(data, []string{def.Properties.Name, "description", def.Properties.Description, "", "", ""})
		for _, p := range def.Parameters {
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "description", p.Description, ""})
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "required", strconv.FormatBool(p.Required), ""})
			if p.DefaultValue != nil {
				data = append(data, []string{def.Properties.Name, "parameters", p.Name, "default", string(p.DefaultValue), ""})
			}
			if p.LinkParameter != "" {
				data = append(data, []string{def.Properties.Name, "parameters", p.Name, "linked_parameter", p.LinkParameter, ""})
			}
			if p.CastDbType != "" {
				data = append(data, []string{def.Properties.Name, "parameters", p.Name, "cast_to_db_type", p.CastDbType, ""})
			}
			if p.ColumnProperties != nil {
				if len(p.ColumnProperties.AllowedTypes) > 0 {
					allowedTypes := strings.Join(p.ColumnProperties.AllowedTypes, ", ")
					data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "allowed_types", allowedTypes})
				}
				isAffected := strconv.FormatBool(p.ColumnProperties.Affected)
				data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "is_affected", isAffected})
				skipOriginalData := strconv.FormatBool(p.ColumnProperties.SkipOriginalData)
				data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "skip_original_data", skipOriginalData})
				skipOnNull := strconv.FormatBool(p.ColumnProperties.SkipOnNull)
				data = append(data, []string{def.Properties.Name, "parameters", p.Name, "column_properties", "skip_on_null", skipOnNull})
			}

		}
	}
	table.AppendBulk(data)
	table.SetRowLine(true)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1, 2, 3})
	table.Render()

	return nil
}

func init() {
	Cmd.Flags().StringVarP(&format, "format", "f", TextFormatName, "output format [text|json]")
}
