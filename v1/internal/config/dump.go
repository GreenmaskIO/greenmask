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

package config

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	mysqlconfig "github.com/greenmaskio/greenmask/v1/internal/mysql/dump/config"
	pgconfig "github.com/greenmaskio/greenmask/v1/internal/pg/dump/config"
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

type MysqlDumpConfig struct {
	Options mysqlconfig.DumpOptions `mapstructure:"options" yaml:"options" json:"options"`
}

type PostgresqlDumpConfig struct {
	Options pgconfig.DumpOptions `mapstructure:"options" yaml:"options" json:"options"`
}

type Options struct {
	IncludeTable     []string `mapstructure:"include-table" yaml:"include-table" json:"include-table"`
	ExcludeTable     []string `mapstructure:"exclude-table" yaml:"exclude-table" json:"exclude-table"`
	IncludeSchema    []string `mapstructure:"include-schema" yaml:"include-schema" json:"include-schema"`
	ExcludeSchema    []string `mapstructure:"exclude-schema" yaml:"exclude-schema" json:"exclude-schema"`
	ExcludeTableData []string `mapstructure:"exclude-table-data" yaml:"exclude-table-data" json:"exclude-table-data"`
	DataOnly         bool     `mapstructure:"data-only" yaml:"data-only" json:"data-only"`
	SchemaOnly       bool     `mapstructure:"schema-only" yaml:"schema-only" json:"schema-only"`
	Jobs             int      `mapstructure:"jobs" yaml:"jobs" json:"jobs"`
	RestoreInOrder   bool     `mapstructure:"restore-in-order" yaml:"restore-in-order" json:"restore-in-order"`
}

type Dump struct {
	Options           Options              `mapstructure:"options" yaml:"options" json:"options"`
	MysqlConfig       MysqlDumpConfig      `mapstructure:"mysql" yaml:"mysql" json:"mysql"`
	PostgresqlConfig  PostgresqlDumpConfig `mapstructure:"postgresql" yaml:"postgresql" json:"postgresql"`
	Transformation    TransformationConfig `mapstructure:"transformation" yaml:"transformation" json:"transformation,omitempty"`
	VirtualReferences []VirtualReference   `mapstructure:"virtual_references" yaml:"virtual_references" json:"virtual_references,omitempty"`
	Tag               []string             `mapstructure:"tag" yaml:"tag" json:"tag,omitempty"`
	Description       string               `mapstructure:"description" yaml:"description" json:"description,omitempty"`
}

func NewDump() Dump {
	return Dump{}
}
