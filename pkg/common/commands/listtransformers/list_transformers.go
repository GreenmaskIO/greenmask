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

// Package listtransformers provides engine-agnostic transformer discovery.
// It maps the internal transformer registry into serialisable config suitable
// for programmatic consumption (gm-backend, REST API, CLI). All
// formatting/printing belongs to the caller.
package listtransformers

import (
	"slices"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/utils"
)

const AnyTypesValue = "any"

// ColumnParameterProperties describes the type constraints of a column-related
// parameter or column container parameter.
type ColumnParameterProperties struct {
	Affected         bool     `json:"affected,omitempty"`
	SupportedTypes   []string `json:"supported_types,omitempty"`
	SupportedClasses []string `json:"supported_type_classes,omitempty"`
	SkipOriginalData bool     `json:"skip_original_data,omitempty"`
	SkipOnNull       bool     `json:"skip_on_null,omitempty"`
}

// ParameterItem is a serialisable representation of one transformer parameter.
type ParameterItem struct {
	Name                string                     `json:"name"`
	Description         string                     `json:"description,omitempty"`
	Required            bool                       `json:"required,omitempty"`
	DefaultValue        string                     `json:"default_value,omitempty"`
	IsColumn            bool                       `json:"is_column,omitempty"`
	IsColumnContainer   bool                       `json:"is_column_container,omitempty"`
	ColumnProperties    *ColumnParameterProperties `json:"column_properties,omitempty"`
	ContainerProperties *ColumnParameterProperties `json:"container_properties,omitempty"`
	LinkColumnParameter string                     `json:"link_column_parameter,omitempty"`
	SupportTemplate     bool                       `json:"support_template,omitempty"`
}

// TransformerItem is a serialisable representation of one registered transformer.
type TransformerItem struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  []ParameterItem `json:"parameters,omitempty"`
}

// Lister queries the transformer registry and returns structured config.
type Lister struct {
	reg *registry.TransformerRegistry
}

// New returns a Lister backed by the given registry.
func New(reg *registry.TransformerRegistry) *Lister {
	return &Lister{reg: reg}
}

// List returns all registered transformers sorted alphabetically by name.
func (l *Lister) List() []TransformerItem {
	items := make([]TransformerItem, 0, len(l.reg.M))
	for _, def := range l.reg.M {
		items = append(items, buildItem(def))
	}
	slices.SortFunc(items, func(a, b TransformerItem) int {
		return strings.Compare(a.Name, b.Name)
	})
	return items
}

// Get returns the named transformer. ok is false when the name is not registered.
func (l *Lister) Get(name string) (TransformerItem, bool) {
	def, ok := l.reg.M[name]
	if !ok {
		return TransformerItem{}, false
	}
	return buildItem(def), true
}

func buildItem(def *utils.TransformerDefinition) TransformerItem {
	params := make([]ParameterItem, 0, len(def.Parameters))
	for _, p := range def.Parameters {
		params = append(params, buildParameterItem(p))
	}
	return TransformerItem{
		Name:        def.Properties.Name,
		Description: def.Properties.Description,
		Parameters:  params,
	}
}

func buildParameterItem(p *parameters.ParameterDefinition) ParameterItem {
	item := ParameterItem{
		Name:                p.Name,
		Description:         p.Description,
		Required:            p.Required,
		IsColumn:            p.IsColumn,
		IsColumnContainer:   p.IsColumnContainer,
		LinkColumnParameter: p.LinkColumnParameter,
		SupportTemplate:     p.SupportTemplate,
	}
	if p.DefaultValue != nil {
		item.DefaultValue = string(p.DefaultValue)
	}
	if p.IsColumn && p.ColumnProperties != nil {
		item.ColumnProperties = &ColumnParameterProperties{
			Affected:         p.ColumnProperties.Affected,
			SupportedTypes:   columnTypes(p.ColumnProperties.AllowedTypes),
			SupportedClasses: columnClasses(p.ColumnProperties.AllowedTypeClasses),
			SkipOriginalData: p.ColumnProperties.SkipOriginalData,
			SkipOnNull:       p.ColumnProperties.SkipOnNull,
		}
	}
	if p.IsColumnContainer && p.ColumnContainerProperties != nil {
		cp := p.ColumnContainerProperties
		item.ContainerProperties = &ColumnParameterProperties{
			SupportedTypes:   columnTypes(cp.AllowedTypes),
			SupportedClasses: columnClasses(cp.AllowedTypeClasses),
		}
	}
	return item
}

func columnTypes(allowed []string) []string {
	if len(allowed) > 0 {
		return allowed
	}
	return []string{AnyTypesValue}
}

func columnClasses(classes []core.TypeClass) []string {
	if len(classes) == 0 {
		return []string{AnyTypesValue}
	}
	res := make([]string, len(classes))
	for i, c := range classes {
		res[i] = string(c)
	}
	return res
}
