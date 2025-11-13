package cmdrun

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"

	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/config"
)

func RunShowTransformers(
	cfg *config.Config,
	format OutputFormat,
	transformerName string,
) error {
	ctx := context.Background()
	ctx = setupContext(ctx, cfg)
	if err := setupInfrastructure(cfg); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	if err := format.Validate(); err != nil {
		return err
	}
	switch format {
	case FormatNameJson:
		err := showTransformerJson(registry.DefaultTransformerRegistry, transformerName)
		if err != nil {
			return fmt.Errorf("error listing transformers: %w", err)
		}
	case FormatNameText:
		err := showTransformerText(registry.DefaultTransformerRegistry, transformerName)
		if err != nil {
			return fmt.Errorf("error listing transformers: %w", err)
		}
	default:
		return fmt.Errorf(`unknown format %s`, format)
	}

	return nil
}

func showTransformerJson(registry *registry.TransformerRegistry, transformerName string) error {
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

func showTransformerText(registry *registry.TransformerRegistry, name string) error {
	var data [][]string
	table := tablewriter.NewWriter(os.Stdout)

	def, err := getTransformerDefinition(registry, name)
	if err != nil {
		return err
	}

	data = append(data, []string{
		def.Properties.Name, "description",
		def.Properties.Description, "",
		"", "",
	})
	for _, p := range def.Parameters {
		data = append(data, []string{
			def.Properties.Name, "parameters",
			p.Name, "description",
			p.Description, "",
		})
		data = append(data, []string{
			def.Properties.Name, "parameters",
			p.Name, "required",
			strconv.FormatBool(p.Required), ""},
		)
		if p.DefaultValue != nil {
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "default",
				string(p.DefaultValue), ""},
			)
		}
		if p.LinkColumnParameter != "" {
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "linked_parameter",
				p.LinkColumnParameter, "",
			})
		}
		if p.IsColumnContainer {
			containerProp := p.ColumnContainerProperties
			types := []string{anyTypesValue}
			typeClasses := []string{anyTypesValue}
			if containerProp != nil && containerProp.ColumnProperties != nil {
				if len(containerProp.ColumnProperties.AllowedTypes) > 0 {
					types = containerProp.ColumnProperties.AllowedTypes
				}
				if len(containerProp.ColumnProperties.AllowedTypeClasses) > 0 {
					typeClasses = make([]string, len(containerProp.ColumnProperties.AllowedTypeClasses))
					for i, tc := range containerProp.ColumnProperties.AllowedTypeClasses {
						typeClasses[i] = string(tc)
					}
				}
			}
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "column_properties",
				"types", strings.Join(types, ", ")},
			)
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "column_properties",
				"type_classes", strings.Join(typeClasses, ", ")},
			)
		}
		if p.ColumnProperties != nil {
			allowedTypes := []string{anyTypesValue}
			if len(p.ColumnProperties.AllowedTypes) > 0 {
				allowedTypes = p.ColumnProperties.AllowedTypes
			}
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "column_properties",
				"types", strings.Join(allowedTypes, ", "),
			})
			allowedTypeClasses := getColumnClasses(p)
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "column_properties",
				"type_classes", strings.Join(allowedTypeClasses, ", "),
			})
			isAffected := strconv.FormatBool(p.ColumnProperties.Affected)
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "column_properties",
				"is_affected", isAffected,
			})
			skipOriginalData := strconv.FormatBool(p.ColumnProperties.SkipOriginalData)
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "column_properties",
				"skip_original_data", skipOriginalData,
			})
			skipOnNull := strconv.FormatBool(p.ColumnProperties.SkipOnNull)
			data = append(data, []string{
				def.Properties.Name, "parameters",
				p.Name, "column_properties",
				"skip_on_null", skipOnNull,
			})
		}
	}

	table.AppendBulk(data)
	table.SetRowLine(true)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1, 2, 3})
	table.Render()

	return nil
}

func getTransformerDefinition(
	registry *registry.TransformerRegistry, name string,
) (*utils.TransformerDefinition, error) {
	def, ok := registry.M[name]
	if ok {
		return def, nil
	}
	return nil, fmt.Errorf("unknown transformer \"%s\"", name)
}
