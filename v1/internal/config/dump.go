package config

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

/*
	Transformation config
*/

type Transformers []TransformerConfig

func (t Transformers) ToTransformerConfig() []models.TransformerConfig {
	transformers := make([]models.TransformerConfig, len(t))
	for i, transformer := range t {
		transformers[i] = transformer.ToTransformerConfig()
	}
	return transformers
}

type Table struct {
	Schema              string            `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	Name                string            `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Query               string            `mapstructure:"query" yaml:"query" json:"query,omitempty"`
	ApplyForInherited   bool              `mapstructure:"apply_for_inherited" yaml:"apply_for_inherited" json:"apply_for_inherited,omitempty"`
	Transformers        Transformers      `mapstructure:"transformers" yaml:"transformers" json:"transformers,omitempty"`
	ColumnsTypeOverride map[string]string `mapstructure:"columns_type_override" yaml:"columns_type_override" json:"columns_type_override,omitempty"`
	SubsetConds         []string          `mapstructure:"subset_conds" yaml:"subset_conds" json:"subset_conds,omitempty"`
	When                string            `mapstructure:"when" yaml:"when" json:"when,omitempty"`
}

func (t Table) ToTableConfig() models.TableConfig {
	table := models.NewTableConfig(
		t.Schema,
		t.Name,
		t.Query,
		t.ApplyForInherited,
		t.Transformers.ToTransformerConfig(),
		t.ColumnsTypeOverride,
		t.SubsetConds,
		t.When,
	)
	return table
}

type TransformerConfig struct {
	Name               string `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	ApplyForReferences bool   `mapstructure:"apply_for_references" yaml:"apply_for_references" json:"apply_for_references,omitempty"`
	// Params - transformation parameters. It might be any type. If structure should be stored as raw json
	// This cannot be parsed with mapstructure due to uncontrollable lowercasing
	// https://github.com/spf13/viper/issues/373
	// Instead we have to use workaround and parse it manually
	//
	// Params attribute decoding is dummy. It is replaced in the runtime internal/utils/config/viper_workaround.go
	// But it is required to leave mapstruicture tag to avoid errors raised by viper and decoder setting
	// ErrorUnused = true. It was set in PR #177 (https://github.com/GreenmaskIO/greenmask/pull/177/files)
	Params StaticParameters `mapstructure:"params" yaml:"-" json:"-"`
	// MetadataParams - encoded transformer parameters - uses only for storing into storage
	// TODO: You need to get rid of it by creating a separate structure for storing metadata in
	//   internal/db/postgres/storage/metadata_json.go
	// this is used only due to https://github.com/spf13/viper/issues/373
	MetadataParams map[string]any    `mapstructure:"-" yaml:"params,omitempty" json:"params,omitempty"`
	DynamicParams  DynamicParameters `mapstructure:"dynamic_params" yaml:"dynamic_params" json:"dynamic_params,omitempty"`
	When           string            `mapstructure:"when" yaml:"when" json:"when,omitempty"`
}

func (tc TransformerConfig) ToTransformerConfig() models.TransformerConfig {
	return models.NewTransformerConfig(
		tc.Name,
		tc.ApplyForReferences,
		tc.Params.ToParamsValue(),
		tc.DynamicParams.ToDynamicParamValue(),
		tc.When,
	)
}

/*
	Virtual references
*/

type ReferencedColumn struct {
	Name       string `mapstructure:"name" json:"name" yaml:"name"`
	Expression string `mapstructure:"expression" json:"expression" yaml:"expression"`
}

type Reference struct {
	Schema           string             `mapstructure:"schema" json:"schema" yaml:"schema"`
	Name             string             `mapstructure:"name" json:"name" yaml:"name"`
	NotNull          bool               `mapstructure:"not_null" json:"not_null" yaml:"not_null"`
	Columns          []ReferencedColumn `mapstructure:"columns" json:"columns" yaml:"columns"`
	PolymorphicExprs []string           `mapstructure:"polymorphic_exprs" json:"polymorphic_exprs" yaml:"polymorphic_exprs"`
}

type VirtualReference struct {
	Schema     string      `mapstructure:"schema" json:"schema" yaml:"schema"`
	Name       string      `mapstructure:"name" json:"name" yaml:"name"`
	References []Reference `mapstructure:"references" json:"references" yaml:"references"`
}

/*
	Dump config (MAIN CONFIG)
*/

type TransformationConfig []Table

func (tc TransformationConfig) ToTransformationConfig() []models.TableConfig {
	tables := make([]models.TableConfig, len(tc))
	for i, table := range tc {
		tables[i] = table.ToTableConfig()
	}
	return tables
}

type Dump struct {
	Options           options              `mapstructure:"options" yaml:"options" json:"options"`
	Transformation    TransformationConfig `mapstructure:"transformation" yaml:"transformation" json:"transformation,omitempty"`
	VirtualReferences []VirtualReference   `mapstructure:"virtual_references" yaml:"virtual_references" json:"virtual_references,omitempty"`
}

func NewDump(o options) Dump {
	return Dump{
		Options: o,
	}
}
