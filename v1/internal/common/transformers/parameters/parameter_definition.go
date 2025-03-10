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
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Unmarshaller func(parameter *ParameterDefinition, driver driver, src models.ParamsValue) (any, error)
type DatabaseTypeUnmarshaler func(driver driver, typeName string, v models.ParamsValue) (any, error)
type RawValueValidator func(p *ParameterDefinition, v models.ParamsValue) (models.ValidationWarnings, error)

const WithoutMaxLength = -1

func DefaultDatabaseTypeUnmarshaler(driver driver, typeName string, v models.ParamsValue) (any, error) {
	return driver.DecodeValueByTypeName(typeName, v)
}

// ColumnProperties - column-like parameter properties that would help to understand the affection on the consistency
type ColumnProperties struct {
	// Nullable - shows that transformer can produce NULL value for the column. Togather with Affected shows that
	// this parameter may generate null values and write it in this column. It only plays with Affected
	Nullable bool `mapstructure:"nullable" json:"nullable,omitempty"`
	// Unique - shows that transformer guarantee that every transformer call the value will be unique. It only plays
	// with Affected
	Unique bool `mapstructure:"unique" json:"unique,omitempty"`
	// Unique - defines max length of the value. It only plays with Affected. Togather with Affected shows
	// that values will not exceed the length of the column. It only plays with Affected
	MaxLength int `mapstructure:"max_length" json:"max_length,omitempty"`
	// Affected - shows assigned column name will be affected after the transformation
	Affected bool `mapstructure:"affected" json:"affected,omitempty"`
	// AllowedTypes - defines all the allowed column types in textual format. If not assigned (nil) then any
	// of the types is valid
	// TODO: AllowedTypes has a problem if we set int and our column is int2, then it cause an error though
	//		 it is workable case. Decide how to define subtype or type "aliases" references.
	//		 Also it has problem with custom type naming because it has schema name and type name. It might be better
	//		 to describe types with {{ schemaName }}.{{ typeName }}, but then we have to implement types classes
	//		 (such as textual, digits, etc.)
	AllowedTypes []string `mapstructure:"allowed_types" json:"allowed_types,omitempty"`
	// SkipOriginalData - Is transformer require original data or not
	SkipOriginalData bool `mapstructure:"skip_original_data" json:"skip_original_data,omitempty"`
	// TODO: Implement SkipOnNull
	// SkipOnNull - transformation for column with NULL is not expected
	SkipOnNull bool `mapstructure:"skip_on_null" json:"skip_on_null"`
}

func NewColumnProperties() *ColumnProperties {
	return &ColumnProperties{
		Nullable:  false,
		MaxLength: WithoutMaxLength,
	}
}

func (cp *ColumnProperties) SetNullable(v bool) *ColumnProperties {
	cp.Nullable = v
	return cp
}

func (cp *ColumnProperties) SetUnique(v bool) *ColumnProperties {
	cp.Unique = v
	return cp
}

func (cp *ColumnProperties) SetMaxLength(v int) *ColumnProperties {
	cp.MaxLength = v
	return cp
}

func (cp *ColumnProperties) SetAllowedColumnTypes(v ...string) *ColumnProperties {
	cp.AllowedTypes = v
	return cp
}

func (cp *ColumnProperties) SetAffected(v bool) *ColumnProperties {
	cp.Affected = v
	return cp
}

func (cp *ColumnProperties) SetSkipOriginalData(v bool) *ColumnProperties {
	cp.SkipOriginalData = v
	return cp
}

func (cp *ColumnProperties) SetSkipOnNull(v bool) *ColumnProperties {
	cp.SkipOnNull = v
	return cp
}

type DynamicModeProperties struct {
	SupportedTypes []string
	Unmarshal      DatabaseTypeUnmarshaler `json:"-"`
}

func NewDynamicModeProperties() *DynamicModeProperties {
	return &DynamicModeProperties{
		//Unmarshal: DefaultDatabaseTypeUnmarshaler,
	}
}

func (dmp *DynamicModeProperties) SetCompatibleTypes(compatibleTypes ...string) *DynamicModeProperties {
	dmp.SupportedTypes = compatibleTypes
	return dmp
}

func (dmp *DynamicModeProperties) SetUnmarshaler(unmarshaler DatabaseTypeUnmarshaler) *DynamicModeProperties {
	dmp.Unmarshal = unmarshaler
	return dmp
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
	// IsColumnContainer - describe is parameter container map or list with multiple columns inside. It allows us to
	IsColumnContainer bool `mapstructure:"is_column_container" json:"is_column_container"`
	// LinkColumnParameter - link with parameter with provided name. This is required if performing raw value encoding
	// depends on the provided column type and/or relies on the database Driver
	LinkColumnParameter string `mapstructure:"link_column_parameter" json:"link_column_parameter,omitempty"`
	// CastDbType - name of PostgreSQL type that would be used for Decoding raw value to the real go type. Is this
	// type does not exist will cause an error
	CastDbType string `mapstructure:"cast_db_type" json:"cast_db_type,omitempty"`
	// GlobalEnvVariable - the nane of the global environment variable that can be used on empty input
	GetFromGlobalEnvVariable string `mapstructure:"get_from_global_env_variable" json:"get_from_global_env_variable,omitempty"`
	// DynamicModeProperties - shows that parameter support dynamic mode and contains allowed types and unmarshaler
	DynamicModeProperties *DynamicModeProperties
	// DefaultValue - default value of the parameter
	DefaultValue models.ParamsValue `mapstructure:"default_value" json:"default_value,omitempty"`
	// ColumnProperties - detail info about expected column properties that may help to diagnose the table schema
	// and perform validation procedure Plays only with IsColumn
	ColumnProperties *ColumnProperties `mapstructure:"column_properties" json:"column_properties,omitempty"`
	// SupportTemplate - shows that parameter supports golang template and might be calculated dynamically
	SupportTemplate bool `mapstructure:"support_template" json:"support_template,omitempty"`
	// Unmarshaller - unmarshal function for the parameter raw data []byte. Using by default json.Unmarshal function
	Unmarshaller Unmarshaller `json:"-"`
	// RawValueValidator - raw value validator function that performs assertion and cause ValidationWarnings if it
	// has violations
	RawValueValidator RawValueValidator `json:"-"`
	// AllowedValues - slice of values which allowed to use
	AllowedValues []models.ParamsValue `mapstructure:"allowed_values" json:"allowed_values,omitempty"`
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

func (p *ParameterDefinition) SetLinkParameter(name string) *ParameterDefinition {
	if p.IsColumn {
		panic("cannot link column parameter with column parameter")
	}
	if p.CastDbType != "" && p.LinkColumnParameter != "" {
		panic("parameter cannot be with two properties cast_db_type and link_column_parameter enabled")
	}
	p.LinkColumnParameter = name
	return p
}

func (p *ParameterDefinition) SetGetFromGlobalEnvVariable(v string) *ParameterDefinition {
	p.GetFromGlobalEnvVariable = v
	return p
}

func (p *ParameterDefinition) SetAllowedValues(v ...models.ParamsValue) *ParameterDefinition {
	p.AllowedValues = v
	return p
}

func (p *ParameterDefinition) SetIsColumn(columnProperties *ColumnProperties) *ParameterDefinition {
	p.IsColumn = true
	p.ColumnProperties = columnProperties
	return p
}

func (p *ParameterDefinition) SetIsColumnContainer(v bool) *ParameterDefinition {
	p.IsColumnContainer = v
	return p
}

func (p *ParameterDefinition) SetUnmarshaler(unmarshaler Unmarshaller) *ParameterDefinition {
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

func (p *ParameterDefinition) SetCastDbType(v string) *ParameterDefinition {
	if p.CastDbType != "" && p.LinkColumnParameter != "" {
		panic("parameter cannot be with two properties cast_db_type and link_column_parameter enabled")
	}
	p.CastDbType = v
	return p
}

func (p *ParameterDefinition) SetDefaultValue(v models.ParamsValue) *ParameterDefinition {
	p.DefaultValue = v
	return p
}

func (p *ParameterDefinition) SetDynamicMode(v *DynamicModeProperties) *ParameterDefinition {
	p.DynamicModeProperties = v
	return p
}
