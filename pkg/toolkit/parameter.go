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

package toolkit

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type Unmarshaller func(parameter *ParameterDefinition, driver *Driver, src ParamsValue) (any, error)
type RawValueValidator func(p *ParameterDefinition, v ParamsValue) (ValidationWarnings, error)

const WithoutMaxLength = -1

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
	// LinkColumnParameter - link with parameter with provided name. This is required if performing raw value encoding
	// depends on the provided column type and/or relies on the database Driver
	LinkColumnParameter string `mapstructure:"link_column_parameter" json:"link_column_parameter,omitempty"`
	// CastDbType - name of PostgreSQL type that would be used for Decoding raw value to the real go type. Is this
	// type does not exist will cause an error
	CastDbType string `mapstructure:"cast_db_type" json:"cast_db_type,omitempty"`
	// DynamicModeSupport - shows that parameter value can be gathered from column value of the current record
	DynamicModeSupport bool `mapstructure:"dynamic_mode_support" json:"dynamic_mode_support,omitempty"`
	// DefaultValue - default value of the parameter
	DefaultValue ParamsValue `mapstructure:"default_value" json:"default_value,omitempty"`
	// ColumnProperties - detail info about expected column properties that may help to diagnose the table schema
	// and perform validation procedure Plays only with IsColumn
	ColumnProperties *ColumnProperties `mapstructure:"column_properties" json:"column_properties,omitempty"`
	// Unmarshaller - unmarshal function for the parameter raw data []byte. Using by default json.Unmarshal function
	Unmarshaller Unmarshaller `json:"-"`
	// RawValueValidator - raw value validator function that performs assertion and cause ValidationWarnings if it
	// has violations
	RawValueValidator RawValueValidator `json:"-"`
	// LinkedParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	// Deprecated
	LinkedColumnParameter *ParameterDefinition `json:"-"`
	// Column - column of the table that was assigned in the parsing procedure according to provided Column name in
	// parameter value. In this case value has textual column name
	// Deprecated
	Column *Column `json:"-"`
	// Driver - initialized used for decoding raw value to database type mentioned in CastDbType
	// Deprecated
	Driver *Driver `mapstructure:"-" json:"-"`
	// value - cached parsed value after Scan or Value
	// Deprecated
	value any
	// rawValue - original raw value received from config
	// Deprecated
	rawValue ParamsValue
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
		Name:               name,
		Description:        description,
		DynamicModeSupport: false,
	}, nil
}

// Deprecated
func (p *ParameterDefinition) RawValue() ParamsValue {
	return p.rawValue
}

// Value - returns parsed value that later might be cast via type assertion or so on
// Deprecated
func (p *ParameterDefinition) Value() (any, error) {
	if p.rawValue == nil {
		return nil, nil
	}

	if p.Unmarshaller != nil {
		// Perform custom unmarshalling
		val, err := p.Unmarshaller(p, p.Driver, p.rawValue)
		if err != nil {
			return false, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		p.value = val
	} else if p.LinkedColumnParameter != nil {
		// Parsing dynamically - default value and type are unknown
		// TODO: Be careful - this may cause an error in Scan func if the the returning value is not a pointer
		val, err := p.Driver.DecodeValueByTypeOid(uint32(p.LinkedColumnParameter.Column.TypeOid), p.rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
		p.value = val
	} else if p.CastDbType != "" {
		val, err := p.Driver.DecodeValueByTypeName(p.CastDbType, p.rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to scan parameter via Driver: %w", err)
		}
		p.value = val
	} else if p.IsColumn {
		p.value = string(p.rawValue)
	} else {
		return nil, errors.New("unknown parsing case: use Scan method instead")
	}

	return p.value, nil
}

// Scan - scan parsed value into received pointer. Param src must be pointer
// Deprecated
func (p *ParameterDefinition) Scan(dest any) (empty bool, err error) {
	p.value = nil
	if dest == nil {
		return false, fmt.Errorf("dest cannot be nil")
	}

	if p.rawValue == nil {
		return true, nil
	}

	p.value = dest
	if p.Unmarshaller != nil {
		// Perform custom unmarshalling
		value, err := p.Unmarshaller(p, p.Driver, p.rawValue)
		if err != nil {
			return false, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		p.value = value
	} else if p.CastDbType != "" {
		// Perform decoding via pgx Driver
		switch p.value.(type) {
		case *time.Time:
			val, err := p.Driver.DecodeValueByTypeName(p.CastDbType, p.rawValue)
			if err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			p.value = &valTime
		default:
			if err := p.Driver.ScanValueByTypeName(p.CastDbType, p.rawValue, p.value); err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
		}
	} else if p.LinkedColumnParameter != nil {

		// Try to scan value using pgx Driver and pgtype defined in the linked column
		if p.LinkedColumnParameter.Column == nil {
			return false, fmt.Errorf("parameter is linked but column was not assigned")
		}

		switch p.value.(type) {
		case *time.Time:
			val, err := p.Driver.DecodeValueByTypeOid(uint32(p.LinkedColumnParameter.Column.TypeOid), p.rawValue)
			if err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
			valTime := val.(time.Time)
			p.value = &valTime
		default:
			if err := p.Driver.ScanValueByTypeOid(uint32(p.LinkedColumnParameter.Column.TypeOid), p.rawValue, p.value); err != nil {
				return false, fmt.Errorf("unable to scan parameter via Driver: %w", err)
			}
		}

	} else {

		switch p.value.(type) {
		case string:
			val := string(p.rawValue)
			p.value = &val
		case *string:
			val := string(p.rawValue)
			p.value = &val
		case time.Duration:
			res, err := time.ParseDuration(string(p.rawValue))
			if err != nil {
				return false, fmt.Errorf("error parsing int64 value: %w", err)
			}
			p.value = &res
		case *time.Duration:
			res, err := time.ParseDuration(string(p.rawValue))
			if err != nil {
				return false, fmt.Errorf("error parsing int64 value: %w", err)
			}
			p.value = &res
		default:
			if err := json.Unmarshal(p.rawValue, p.value); err != nil {
				return false, fmt.Errorf("unable to unmarshal value: %w", err)
			}
		}

	}

	if p.value == nil {
		return false, nil
	}
	return false, ScanPointer(p.value, dest)
}

func (p *ParameterDefinition) SetLinkParameter(name string) *ParameterDefinition {
	if p.IsColumn {
		panic("cannot link column parameter with column parameter")
	}
	p.LinkColumnParameter = name
	return p
}

func (p *ParameterDefinition) SetIsColumn(columnProperties *ColumnProperties) *ParameterDefinition {
	p.IsColumn = true
	p.ColumnProperties = columnProperties
	return p
}

func (p *ParameterDefinition) SetUnmarshaller(unmarshaller Unmarshaller) *ParameterDefinition {
	p.Unmarshaller = unmarshaller
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

func (p *ParameterDefinition) SetCastDbType(v string) *ParameterDefinition {
	p.CastDbType = v
	return p
}

func (p *ParameterDefinition) SetDefaultValue(v ParamsValue) *ParameterDefinition {
	p.DefaultValue = v
	return p
}

func (p *ParameterDefinition) SetDynamicModeSupport(v bool) *ParameterDefinition {
	p.DynamicModeSupport = v
	return p
}

// Deprecated
func (p *ParameterDefinition) Copy() *ParameterDefinition {
	cp := *p
	cp.value = nil
	cp.rawValue = []byte{}
	return &cp
}

// Deprecated
func (p *ParameterDefinition) Init(driver *Driver, types []*Type, params []*ParameterDefinition, rawValue ParamsValue) (ValidationWarnings, error) {
	var warnings ValidationWarnings
	p.Driver = driver
	p.rawValue = nil
	p.rawValue = slices.Clone(rawValue)

	if rawValue == nil {
		if p.Required {
			return ValidationWarnings{
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("parameter is required").
						AddMeta("ParameterName", p.Name),
				},
				nil
		} else if p.DefaultValue != nil {
			p.rawValue = p.DefaultValue
		}
	}

	if p.RawValueValidator != nil {
		w, err := p.RawValueValidator(p, p.rawValue)
		if err != nil {
			return nil, fmt.Errorf("error performing parameter raw value validation: %w", err)
		}
		for _, w := range warnings {
			w.AddMeta("ParameterName", p.Name)
		}
		warnings = append(warnings, w...)
		if w.IsFatal() {
			return warnings, nil
		}
	}

	if p.LinkColumnParameter != "" {
		idx := slices.IndexFunc(params, func(parameter *ParameterDefinition) bool {
			return parameter.Name == p.LinkColumnParameter
		})
		if idx == -1 {
			panic(fmt.Sprintf(`parameter with name "%s" is not found`, p.LinkColumnParameter))
		}
		p.LinkedColumnParameter = params[idx]
	}

	if p.IsColumn {
		columnName := string(p.rawValue)
		p.value = columnName
		_, column, ok := driver.GetColumnByName(columnName)
		if !ok {
			return ValidationWarnings{
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("column does not exist").
						AddMeta("ColumnName", columnName).
						AddMeta("ParameterName", p.Name),
				},
				nil
		}
		pgType, ok := driver.SharedTypeMap.TypeForOID(uint32(column.TypeOid))
		if !ok {
			return ValidationWarnings{
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("unsupported column type: type is not found").
						AddMeta("ColumnName", columnName).
						AddMeta("TypeName", column.TypeName).
						AddMeta("AllowedDbTypes", p.ColumnProperties.AllowedTypes).
						AddMeta("ParameterName", p.Name),
				},
				nil

		}

		idx := slices.IndexFunc(types, func(t *Type) bool {
			return t.Oid == column.TypeOid
		})
		var t *Type
		var pgRootType *pgtype.Type
		if idx != -1 {
			t = types[idx]
			pgRootType, ok = driver.SharedTypeMap.TypeForOID(uint32(t.RootBuiltInTypeOid))
			if !ok {
				return nil, fmt.Errorf("unknown root type %d", t.RootBuiltInTypeOid)
			}
		}

		if p.ColumnProperties != nil && len(p.ColumnProperties.AllowedTypes) > 0 {

			// Check that one of original column type or root base type or overridden type is suitable for allowed types
			if !slices.Contains(p.ColumnProperties.AllowedTypes, pgType.Name) &&
				!(pgRootType != nil && slices.Contains(p.ColumnProperties.AllowedTypes, pgRootType.Name)) {
				return ValidationWarnings{
						NewValidationWarning().
							SetSeverity(ErrorValidationSeverity).
							SetMsg("unsupported column type").
							AddMeta("ColumnName", columnName).
							AddMeta("ColumnType", pgType.Name).
							AddMeta("AllowedDbTypes", p.ColumnProperties.AllowedTypes).
							AddMeta("ParameterName", p.Name),
					},
					nil
			}
		}
		p.Column = column
	}

	if p.ColumnProperties != nil {
		for _, at := range p.ColumnProperties.AllowedTypes {
			_, ok := driver.SharedTypeMap.TypeForName(at)
			if !ok {
				warnings = append(warnings, NewValidationWarning().
					SetSeverity(WarningValidationSeverity).
					AddMeta("ParameterName", p.Name).
					AddMeta("ItemTypeName", at).
					AddMeta("TransformerAllowedTypes", p.ColumnProperties.AllowedTypes).
					SetMsgf(`allowed type with name %s is not found`, at))
			}
		}
	}
	if p.CastDbType != "" {
		_, ok := driver.SharedTypeMap.TypeForName(p.CastDbType)
		if !ok {
			return ValidationWarnings{
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						AddMeta("ParameterName", p.Name).
						AddMeta("CastDbType", p.CastDbType).
						AddMeta("TransformerAllowedTypes", p.ColumnProperties.AllowedTypes).
						SetMsg(`cannot perform parameter parsing: unknown type cast type: check transformer implementation or ensure your DB has this type`),
				},
				nil
		}
	}

	return warnings, nil
}

func InitParameters(
	driver *Driver, rawParams map[string]ParamsValue, paramDef []*ParameterDefinition, types []*Type,
) (map[string]*ParameterDefinition, ValidationWarnings, error) {
	if rawParams == nil && len(paramDef) > 0 {
		return nil, ValidationWarnings{
			NewValidationWarning().
				SetMsg("parameters are required: received empty").
				SetSeverity("error"),
		}, nil
	}

	var pd []*ParameterDefinition
	params := make(map[string]*ParameterDefinition, len(paramDef))
	for _, p := range paramDef {
		cp := p.Copy()
		params[p.Name] = cp
		pd = append(pd, cp)
	}

	var totalWarnings ValidationWarnings
	for _, p := range params {
		warnings, err := p.Init(driver, types, pd, rawParams[p.Name])
		if err != nil {
			return nil, nil, fmt.Errorf("parameter %s parsing error: %w", p.Name, err)
		}
		if len(warnings) > 0 {
			totalWarnings = append(totalWarnings, warnings...)
			if totalWarnings.IsFatal() {
				return nil, totalWarnings, nil
			}
		}
	}
	return params, totalWarnings, nil
}

func InitParametersV2(
	driver *Driver, paramDef []*ParameterDefinition, staticValues map[string]ParamsValue,
	dynamicValues map[string]*DynamicParamValue,
) (map[string]Parameterizer, ValidationWarnings, error) {

	var requiredParametersCount int

	for _, pd := range paramDef {
		if pd.Required {
			requiredParametersCount++
		}
	}

	if len(staticValues)+len(dynamicValues) == 0 && requiredParametersCount > 0 {
		return nil, ValidationWarnings{
			NewValidationWarning().
				SetMsg("parameters are required: received empty").
				AddMeta("RequiredParametersCount", requiredParametersCount).
				SetSeverity(ErrorValidationSeverity),
		}, nil
	}

	// Check is there unknown parameters name received in static or dynamic values
	var warnings ValidationWarnings
	for name := range staticValues {
		if !slices.ContainsFunc(paramDef, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("received unknown parameter").
					AddMeta("ParameterName", name),
			)
		}

		// Check that value is static and dynamic value did not receive together
		if _, ok := dynamicValues[name]; ok {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("parameter value must be only static or dynamic at the same time").
					AddMeta("ParameterName", name),
			)
		}
	}

	for name := range dynamicValues {
		if !slices.ContainsFunc(paramDef, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("received unknown parameter").
					AddMeta("ParameterName", name),
			)
		}
		if _, ok := staticValues[name]; ok {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("parameter value must be only static or dynamic at the same time").
					AddMeta("ParameterName", name),
			)
		}
	}

	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	// Algorithm
	// 0. Find and divide column parameters from the others
	// 1. Initialized static parameters - first column, then the others
	// 2. Initialize dynamic parameters

	var columnParamsDef []*ParameterDefinition
	var otherParamsDef []*ParameterDefinition
	for _, pd := range paramDef {
		if pd.IsColumn {
			columnParamsDef = append(columnParamsDef, pd)
		} else {
			otherParamsDef = append(otherParamsDef, pd)
		}
	}

	// Initialize column parameters
	params := make(map[string]Parameterizer, len(paramDef))
	columnParams := make(map[string]*StaticParameter)
	for _, pd := range columnParamsDef {
		// try to get the static value
		value, ok := staticValues[pd.Name]
		if ok {
			// TODO: Enrich parameters with ParameterName in Meta
			sp := NewStaticParameter(pd, driver)
			initWarns, err := sp.Init(nil, value)
			if err != nil {
				return nil, warnings, fmt.Errorf("error initializing \"%s\" parameter: %w", pd.Name, err)
			}
			for _, w := range initWarns {
				w.AddMeta("ParameterName", pd.Name)
			}
			warnings = append(warnings, initWarns...)
			params[pd.Name] = sp
			columnParams[pd.Name] = sp
		} else {
			_, ok = dynamicValues[pd.Name]
			if ok {
				warnings = append(
					warnings,
					NewValidationWarning().
						SetSeverity(ErrorValidationSeverity).
						SetMsg("column parameter cannot work in dynamic mode"),
				)
			}
		}
	}

	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	for _, pd := range otherParamsDef {
		dynamicValue, ok := dynamicValues[pd.Name]
		if ok {
			dp := NewDynamicParameter(pd, driver)
			initWarns, err := dp.Init(columnParams, dynamicValue)
			for _, w := range initWarns {
				w.AddMeta("ParameterName", pd.Name)
			}
			warnings = append(warnings, initWarns...)
			if err != nil {
				return nil, warnings, fmt.Errorf("error initializing static parameter \"%s\": %w", pd.Name, err)
			}
			params[pd.Name] = dp
			continue
		}

		staticValue := staticValues[pd.Name]
		sp := NewStaticParameter(pd, driver)
		initWarns, err := sp.Init(columnParams, staticValue)
		for _, w := range initWarns {
			w.AddMeta("ParameterName", pd.Name)
		}
		warnings = append(warnings, initWarns...)
		if err != nil {
			return nil, warnings, fmt.Errorf("error initializing static parameter \"%s\": %w", pd.Name, err)
		}
		params[pd.Name] = sp
	}

	return params, warnings, nil

}
