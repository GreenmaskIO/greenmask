package models

import "slices"

const WithoutMaxLength = -1

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
	// of the types is valid.
	// TODO: AllowedTypes has a problem if we set int and our column is int2, then it cause an error though
	//		 it is workable case. Decide how to define subtype or type "aliases" references.
	//		 Also it has problem with custom type naming because it has schema name and type name. It might be better
	//		 to describe types with {{ schemaName }}.{{ typeName }}, but then we have to implement types classes
	//		 (such as textual, digits, etc.)
	AllowedTypes []string `mapstructure:"allowed_types" json:"allowed_types,omitempty"`
	// AllowedTypeClasses - defines all the allowed column type classes. If not assigned (nil) then any
	// of the types is valid.
	AllowedTypeClasses []TypeClass `mapstructure:"allowed_type_classes" json:"allowed_type_classes,omitempty"`
	// DeniedTypes - defines all the excluded column type classes. If not assigned (nil) then any
	// of the types is valid.
	DeniedTypes []string `mapstructure:"denied_types" json:"denied_types,omitempty"`
	// DeniedTypeClasses - defines all the excluded column type classes. If not assigned (nil) then any
	// of the types is valid.
	DeniedTypeClasses []TypeClass `mapstructure:"denied_type_classes" json:"denied_type_classes,omitempty"`
	// SkipOriginalData - Is transformer require original data or not.
	SkipOriginalData bool `mapstructure:"skip_original_data" json:"skip_original_data,omitempty"`
	// SkipOnNull - transformation for column with NULL is not expected.
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

func (cp *ColumnProperties) SetAllowedColumnTypeClasses(v ...TypeClass) *ColumnProperties {
	cp.AllowedTypeClasses = v
	return cp
}

func (cp *ColumnProperties) SetDeniedColumnTypes(v ...string) *ColumnProperties {
	cp.DeniedTypes = v
	return cp
}

func (cp *ColumnProperties) SetDeniedColumnTypeClasses(v ...TypeClass) *ColumnProperties {
	cp.DeniedTypeClasses = v
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

func (cp *ColumnProperties) IsColumnTypeAllowed(v string) bool {
	if len(cp.AllowedTypes) > 0 && !slices.Contains(cp.AllowedTypes, v) {
		return false
	}
	return true
}

func (cp *ColumnProperties) IsColumnTypeDenied(v string) bool {
	if len(cp.DeniedTypes) > 0 && slices.Contains(cp.DeniedTypes, v) {
		return true
	}
	return false
}

func (cp *ColumnProperties) IsColumnTypeClassAllowed(v TypeClass) bool {
	if len(cp.AllowedTypeClasses) > 0 && !slices.Contains(cp.AllowedTypeClasses, v) {
		return false
	}
	return true
}

func (cp *ColumnProperties) IsColumnTypeClassDenied(v TypeClass) bool {
	if len(cp.DeniedTypeClasses) > 0 && slices.Contains(cp.DeniedTypeClasses, v) {
		return true
	}
	return false
}
