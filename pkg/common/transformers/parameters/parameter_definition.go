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

package parameters

import (
	"context"

	commonininterfaces "github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commoninmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type Unmarshaler func(parameter *ParameterDefinition, driver commonininterfaces.DBMSDriver, src commoninmodels.ParamsValue) (any, error)
type DatabaseTypeUnmarshaler func(driver commonininterfaces.DBMSDriver, typeName string, v commoninmodels.ParamsValue) (any, error)
type RawValueValidator func(ctx context.Context, p *ParameterDefinition, v commoninmodels.ParamsValue) error

type ColumnContainerUnmarshaler func(ctx context.Context, parameter *ParameterDefinition, data commoninmodels.ParamsValue) ([]ColumnContainer, error)

func DefaultDatabaseTypeUnmarshaler(driver commonininterfaces.DBMSDriver, typeName string, v commoninmodels.ParamsValue) (any, error) {
	return driver.DecodeValueByTypeName(typeName, v)
}

type DynamicModeProperties struct {
	*commoninmodels.ColumnProperties
	Unmarshal DatabaseTypeUnmarshaler `json:"-"`
}

func NewDynamicModeProperties() *DynamicModeProperties {
	return &DynamicModeProperties{
		ColumnProperties: commoninmodels.NewColumnProperties(),
	}
}

func (m *DynamicModeProperties) SetColumnProperties(v *commoninmodels.ColumnProperties) *DynamicModeProperties {
	m.ColumnProperties = v
	return m
}

func (m *DynamicModeProperties) SetUnmarshaler(unmarshaler DatabaseTypeUnmarshaler) *DynamicModeProperties {
	m.Unmarshal = unmarshaler
	return m
}

// ParameterDefinition - wide parameter entity definition that contains properties that allows to check schema, find affection,
// cast variable using some features and so on. It may be defined and assigned ot the TransformerDefinition of the transformer
// if transformer has any parameters
type ParameterDefinition struct {
	// Name - name of the parameter. Must be unique in the whole Transformer parameters slice
	Name string `mapstructure:"name" json:"name"`
	// Description - description of the parameter. Should contain the brief info about parameter
	Description string `mapstructure:"description" json:"description"`
	// Required - shows that parameter is required, and we expect we have to receive this value from config.
	// Event when DefaultValue is defined it will case error
	Required bool `mapstructure:"required" json:"required"`
	// IsColumn - shows is this parameter column related. If so ColumnProperties must be defined and assigned
	// otherwise it may cause an unhandled behaviour
	IsColumn bool `mapstructure:"is_column" json:"is_column"`
	// IsColumnContainer - describe is parameter container map or list with multiple columns inside. It allows to
	// use this parameter as a container for multiple columns and apply changes to all columns inside.
	IsColumnContainer bool `mapstructure:"is_column_container" json:"is_column_container"`
	// ColumnContainerProperties - properties of the column container that describes allowed types and unmarshaler.
	ColumnContainerProperties *ColumnContainerProperties `mapstructure:"column_container_properties" json:"column_container_properties,omitempty"`
	// LinkColumnParameter - link with parameter with provided name. This is required if performing raw value encoding
	// depends on the provided column type and/or relies on the database Driver
	LinkColumnParameter string `mapstructure:"link_column_parameter" json:"link_column_parameter,omitempty"`
	// DynamicModeProperties - shows that parameter support dynamic mode and contains allowed types and unmarshaler
	DynamicModeProperties *DynamicModeProperties
	// DefaultValue - default value of the parameter
	DefaultValue commoninmodels.ParamsValue `mapstructure:"default_value" json:"default_value,omitempty"`
	// ColumnProperties - detail info about expected column properties that may help to diagnose the table schema
	// and perform validation procedure Plays only with IsColumn
	ColumnProperties *commoninmodels.ColumnProperties `mapstructure:"column_properties" json:"column_properties,omitempty"`
	// SupportTemplate - shows that parameter supports golang template and might be calculated dynamically
	SupportTemplate bool `mapstructure:"support_template" json:"support_template,omitempty"`
	// Unmarshaller - unmarshal function for the parameter raw data []byte. Using by default json.Unmarshal function
	Unmarshaller Unmarshaler `json:"-"`
	// RawValueValidator - raw value validator function that performs assertion and cause ValidationWarnings if it
	// has violations
	RawValueValidator RawValueValidator `json:"-"`
	// AllowedValues - slice of values which allowed to use
	AllowedValues []commoninmodels.ParamsValue `mapstructure:"allowed_values" json:"allowed_values,omitempty"`
	// GlobalEnvVariable - the nane of the global environment variable that can be used on empty input
	GetFromGlobalEnvVariable string `mapstructure:"get_from_global_env_variable" json:"get_from_global_env_variable,omitempty"`
}

func MustNewParameterDefinition(name string, description string) *ParameterDefinition {
	p, err := NewParameterDefinition(name, description)
	if err != nil {
		panic(err)
	}
	return p
}

func NewParameterDefinition(name string, description string) (*ParameterDefinition, error) {
	return &ParameterDefinition{
		Name:        name,
		Description: description,
	}, nil
}

// LinkParameter - links parameter with the column parameter by name. If set it uses the column type
// to decode raw value to the real go type.
func (p *ParameterDefinition) LinkParameter(name string) *ParameterDefinition {
	if p.IsColumn {
		panic("cannot link column parameter with column parameter")
	}
	p.LinkColumnParameter = name
	return p
}

func (p *ParameterDefinition) SetAllowedValues(v ...commoninmodels.ParamsValue) *ParameterDefinition {
	p.AllowedValues = v
	return p
}

func (p *ParameterDefinition) SetIsColumn(columnProperties *commoninmodels.ColumnProperties) *ParameterDefinition {
	p.IsColumn = true
	p.ColumnProperties = columnProperties
	return p
}

func (p *ParameterDefinition) SetIsColumnContainer(v bool) *ParameterDefinition {
	p.IsColumnContainer = v
	return p
}

type ColumnContainer interface {
	ColumnName() string
	IsAffected() bool
}

type ColumnContainerProperties struct {
	*commoninmodels.ColumnProperties
	Unmarshaler ColumnContainerUnmarshaler `mapstructure:"-" json:"-"`
}

func NewColumnContainerProperties() *ColumnContainerProperties {
	return &ColumnContainerProperties{
		ColumnProperties: commoninmodels.NewColumnProperties(),
	}
}

func (cp *ColumnContainerProperties) SetUnmarshaler(unmarshaler ColumnContainerUnmarshaler) *ColumnContainerProperties {
	cp.Unmarshaler = unmarshaler
	return cp
}

func (cp *ColumnContainerProperties) SetColumnProperties(v *commoninmodels.ColumnProperties) *ColumnContainerProperties {
	cp.ColumnProperties = v
	return cp
}

func (p *ParameterDefinition) SetColumnContainer(prop *ColumnContainerProperties) *ParameterDefinition {
	p.ColumnContainerProperties = prop
	p.IsColumnContainer = true
	return p
}

func (p *ParameterDefinition) SetUnmarshaler(unmarshaler Unmarshaler) *ParameterDefinition {
	p.Unmarshaller = unmarshaler
	return p
}

func (p *ParameterDefinition) SetRawValueValidator(validator RawValueValidator) *ParameterDefinition {
	p.RawValueValidator = validator
	return p
}

func (p *ParameterDefinition) SetRequired(v bool) *ParameterDefinition {
	// Checking database types exists
	p.Required = v
	return p
}

func (p *ParameterDefinition) SetSupportTemplate(v bool) *ParameterDefinition {
	p.SupportTemplate = v
	return p
}

func (p *ParameterDefinition) SetDefaultValue(v commoninmodels.ParamsValue) *ParameterDefinition {
	p.DefaultValue = v
	return p
}

func (p *ParameterDefinition) SetDynamicMode(v *DynamicModeProperties) *ParameterDefinition {
	p.DynamicModeProperties = v
	return p
}

func (p *ParameterDefinition) SetGetFromGlobalEnvVariable(v string) *ParameterDefinition {
	p.GetFromGlobalEnvVariable = v
	return p
}
