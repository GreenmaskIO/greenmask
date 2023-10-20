package list_transformers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	Cmd = &cobra.Command{
		Use: "list-transformers",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Err(err).Msg("")
			}

			if err := run(); err != nil {
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

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := custom.BootstrapCustomTransformers(ctx, utils.DefaultTransformerRegistry, Config.CustomTransformers)
	if err != nil {
		return fmt.Errorf("error registering custom transformer: %w", err)
	}

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
	var transformers []*utils.Definition
	for _, def := range registry.M {
		transformers = append(transformers, def)
	}
	if err := json.NewEncoder(os.Stdout).Encode(transformers); err != nil {
		return err
	}
	return nil
}

func listTransformersText(registry *utils.TransformerRegistry) error {

	var data [][]string
	table := tablewriter.NewWriter(os.Stdout)

	for _, def := range registry.M {
		data = append(data, []string{def.Properties.Name, "description", def.Properties.Description, "", "", ""})
		for _, p := range def.Parameters {
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "description", p.Description, ""})
			data = append(data, []string{def.Properties.Name, "parameters", p.Name, "required", strconv.FormatBool(p.Required), ""})
			//data = append(data, []string{def.Properties.Name, "parameters", p.Name, "is_column", strconv.FormatBool(p.IsColumn), ""})
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
