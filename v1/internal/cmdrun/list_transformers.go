package cmdrun

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/olekukonko/tablewriter"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/config"
)

type OutputFormat string

const (
	FormatNameJson OutputFormat = "json"
	FormatNameText OutputFormat = "text"
)

func (m OutputFormat) Validate() error {
	switch m {
	case FormatNameJson, FormatNameText:
		return nil
	default:
		return fmt.Errorf("format '%s': %w", m, commonmodels.ErrValueValidationFailed)
	}
}

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

func RunListTransformers(cfg *config.Config, format OutputFormat) error {
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
		err := listTransformersJson(registry.DefaultTransformerRegistry)
		if err != nil {
			return fmt.Errorf("error listing transformers: %w", err)
		}
	case FormatNameText:
		err := listTransformersText(registry.DefaultTransformerRegistry)
		if err != nil {
			return fmt.Errorf("error listing transformers: %w", err)
		}
	default:
		return fmt.Errorf(`unknown format %s`, format)
	}

	return nil
}

func listTransformersJson(registry *registry.TransformerRegistry) error {
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

func listTransformersText(registry *registry.TransformerRegistry) error {

	var data [][]string
	table := tablewriter.NewWriter(os.Stdout)
	var names []string
	for name := range registry.M {
		names = append(names, name)
	}
	slices.Sort(names)
	table.SetHeader([]string{
		"name",
		"description",
		"column parameter name",
		"supported types",
		"supported type classes",
	})
	for _, name := range names {
		def := registry.M[name]
		for _, p := range def.Parameters {
			if !p.IsColumn && !p.IsColumnContainer {
				continue
			}
			data = append(data, []string{
				def.Properties.Name,
				def.Properties.Description,
				p.Name,
				strings.Join(getColumnTypes(p), ", "),
				strings.Join(getColumnClasses(p), ", "),
			})
		}
	}

	table.AppendBulk(data)
	table.SetRowLine(true)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1})
	table.Render()

	return nil
}

func getColumnTypes(p *parameters.ParameterDefinition) []string {
	if p.ColumnProperties != nil && len(p.ColumnProperties.AllowedTypes) > 0 {
		return p.ColumnProperties.AllowedTypes
	}
	return []string{anyTypesValue}
}

func getColumnClasses(p *parameters.ParameterDefinition) []string {
	if p.ColumnProperties == nil || len(p.ColumnProperties.AllowedTypeClasses) == 0 {
		return []string{anyTypesValue}
	}
	res := make([]string, len(p.ColumnProperties.AllowedTypeClasses))
	for i, tc := range p.ColumnProperties.AllowedTypeClasses {
		res[i] = string(tc)
	}
	return res
}
