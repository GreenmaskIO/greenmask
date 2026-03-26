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
	"time"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	mysqlcommonconfig "github.com/greenmaskio/greenmask/pkg/mysql/config"
)

/*
	Transformation config
*/

const (
	DefaultMaxAllowedPacket       = 0
	DefaultMaxInsertStatementSize = 4 * 1024 * 1024
)

type Transformers []TransformerConfig

func (t Transformers) ToTransformerConfig() []commonmodels.TransformerConfig {
	transformers := make([]commonmodels.TransformerConfig, len(t))
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

func (t Table) ToTableConfig() commonmodels.TableConfig {
	table := commonmodels.NewTableConfig(
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

func (tc TransformerConfig) ToTransformerConfig() commonmodels.TransformerConfig {
	return commonmodels.NewTransformerConfig(
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

func (tc TransformationConfig) ToTransformationConfig() []commonmodels.TableConfig {
	tables := make([]commonmodels.TableConfig, len(tc))
	for i, table := range tc {
		tables[i] = table.ToTableConfig()
	}
	return tables
}

type MysqlDumpConfig struct {
	mysqlcommonconfig.ConnectionOpts `mapstructure:",squash" json:",squash,omitempty"` //nolint:staticcheck
	DumpFormat                       commonmodels.DumpFormat                           `mapstructure:"dump-format" json:"dump_format,omitempty"`                             // Format for data dump (csv or insert)
	MaxInsertStatementSize           int                                               `mapstructure:"max-insert-statement-size" json:"max_insert_statement_size,omitempty"` // Max size of a single insert statement in bytes
	NoTablespaces                    bool                                              `mapstructure:"no-tablespaces" json:"no_databases,omitempty"`                         // Exclude tablespace information (--no-tablespaces)
	PoolHeartbeatInterval            time.Duration                                     `mapstructure:"pool-heartbeat-interval" json:"pool_heartbeat_interval,omitempty"`     // Interval for connection pool heartbeat in seconds
	PoolHeartbeatTimeout             time.Duration                                     `mapstructure:"pool-heartbeat-timeout" json:"pool_heartbeat_timeout,omitempty"`       // Timeout for connection pool heartbeat in seconds
	VendorOptions                    []string                                          `mapstructure:"vendor-options" json:"vendor-options,omitempty"`                       // Arbitrary options for mysqldump
}

type PostgresqlDumpConfig struct {
	Options any `mapstructure:"options" yaml:"options" json:"options"`
}

type CommonDumpOptions struct {
	IncludeDatabase        []string `mapstructure:"include-database" yaml:"include-database" json:"include-database"`
	ExcludeDatabase        []string `mapstructure:"exclude-database" yaml:"exclude-database" json:"exclude-database"`
	IncludeSchema          []string `mapstructure:"include-schema" yaml:"include-schema" json:"include-schema"`
	ExcludeSchema          []string `mapstructure:"exclude-schema" yaml:"exclude-schema" json:"exclude-schema"`
	IncludeTableData       []string `mapstructure:"include-table-data" yaml:"include-table-data" json:"include-table-data"`
	ExcludeTableData       []string `mapstructure:"exclude-table-data" yaml:"exclude-table-data" json:"exclude-table-data"`
	IncludeTable           []string `mapstructure:"include-table" yaml:"include-table" json:"include-table"`
	ExcludeTable           []string `mapstructure:"exclude-table" yaml:"exclude-table" json:"exclude-table"`
	IncludeTableDefinition []string `mapstructure:"include-table-definition" yaml:"include-table-definition" json:"include-table-definition"`
	ExcludeTableDefinition []string `mapstructure:"exclude-table-definition" yaml:"exclude-table-definition" json:"exclude-table-definition"`
	DataOnly               bool     `mapstructure:"data-only" yaml:"data-only" json:"data-only"`
	SchemaOnly             bool     `mapstructure:"schema-only" yaml:"schema-only" json:"schema-only"`
	Options                []string `mapstructure:"options" yaml:"options" json:"options"`
	Jobs                   int      `mapstructure:"jobs" yaml:"jobs" json:"jobs"`
	Compress               bool     `mapstructure:"compress" yaml:"compress" json:"compress"`
	Pgzip                  bool     `mapstructure:"pgzip" yaml:"pgzip" json:"pgzip"`
}

func (o *CommonDumpOptions) GetIncludedTables() []string {
	return o.IncludeTable
}

func (o *CommonDumpOptions) GetExcludedTables() []string {
	return o.ExcludeTable
}

func (o *CommonDumpOptions) GetExcludedDatabases() []string {
	return o.ExcludeDatabase
}

func (o *CommonDumpOptions) GetIncludedDatabases() []string {
	return o.IncludeDatabase
}

func (o *CommonDumpOptions) GetExcludedSchemas() []string {
	return o.ExcludeSchema
}

func (o *CommonDumpOptions) GetIncludedSchemas() []string {
	return o.IncludeSchema
}

func (o *CommonDumpOptions) GetExcludedTableData() []string {
	return o.ExcludeTableData
}

func (o *CommonDumpOptions) GetIncludedTableData() []string {
	return o.IncludeTableData
}

func (o *CommonDumpOptions) GetIncludedTableDefinitions() []string {
	return o.IncludeTableDefinition
}

func (o *CommonDumpOptions) GetExcludedTableDefinitions() []string {
	return o.ExcludeTableDefinition
}

type Dump struct {
	Options           CommonDumpOptions    `mapstructure:"options" yaml:"options" json:"options"`
	MysqlConfig       MysqlDumpConfig      `mapstructure:"mysql" yaml:"mysql" json:"mysql"`
	PostgresqlConfig  PostgresqlDumpConfig `mapstructure:"postgresql" yaml:"postgresql" json:"postgresql"`
	Transformation    TransformationConfig `mapstructure:"transformation" yaml:"transformation" json:"transformation,omitempty"`
	VirtualReferences []VirtualReference   `mapstructure:"virtual_references" yaml:"virtual_references" json:"virtual_references,omitempty"`
	Tag               []string             `mapstructure:"tag" yaml:"tag" json:"tag,omitempty"`
	Description       string               `mapstructure:"description" yaml:"description" json:"description,omitempty"`
}

func NewDump() Dump {
	return Dump{
		MysqlConfig: MysqlDumpConfig{
			ConnectionOpts: mysqlcommonconfig.ConnectionOpts{
				MaxAllowedPacket: DefaultMaxAllowedPacket,
			},
			MaxInsertStatementSize: DefaultMaxInsertStatementSize,
			DumpFormat:             commonmodels.DumpFormatInsert,
		},
		Options: CommonDumpOptions{
			Compress: true,
			Pgzip:    true,
			Jobs:     1,
		},
	}
}
