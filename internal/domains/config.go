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

package domains

import (
	"sync"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/storages/s3"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var (
	Cfg  *Config
	once sync.Once
)

const (
	defaultDirectoryStoragePath = "/tmp"
	defaultStorageType          = "directory"
)

func NewConfig() *Config {
	once.Do(
		func() {
			Cfg = &Config{
				Common: Common{
					TempDirectory: defaultDirectoryStoragePath,
				},
				Storage: StorageConfig{
					Type:      defaultStorageType,
					S3:        s3.NewConfig(),
					Directory: directory.NewConfig(),
				},
			}
		},
	)
	return Cfg
}

type Config struct {
	Common             Common                          `mapstructure:"common" yaml:"common" json:"common"`
	Log                LogConfig                       `mapstructure:"log" yaml:"log" json:"log"`
	Storage            StorageConfig                   `mapstructure:"storage" yaml:"storage" json:"storage"`
	Dump               Dump                            `mapstructure:"dump" yaml:"dump" json:"dump"`
	Validate           Validate                        `mapstructure:"validate" yaml:"validate" json:"validate"`
	Restore            Restore                         `mapstructure:"restore" yaml:"restore" json:"restore"`
	CustomTransformers []*custom.TransformerDefinition `mapstructure:"custom_transformers" yaml:"custom_transformers" json:"custom_transformers,omitempty"`
}

type Validate struct {
	Tables           []string `mapstructure:"tables" yaml:"tables" json:"tables,omitempty"`
	Data             bool     `mapstructure:"data" yaml:"data" json:"data,omitempty"`
	Diff             bool     `mapstructure:"diff" yaml:"diff" json:"diff,omitempty"`
	Schema           bool     `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	RowsLimit        uint64   `mapstructure:"rows_limit" yaml:"rows_limit" json:"rows_limit,omitempty"`
	ResolvedWarnings []string `mapstructure:"resolved_warnings" yaml:"resolved_warnings" json:"resolved_warnings,omitempty"`
	TableFormat      string   `mapstructure:"table_format" yaml:"table_format" json:"table_format,omitempty"`
	Format           string   `mapstructure:"format" yaml:"format" json:"format,omitempty"`
	OnlyTransformed  bool     `mapstructure:"transformed_only" yaml:"transformed_only" json:"transformed_only,omitempty"`
	Warnings         bool     `mapstructure:"warnings" yaml:"warnings" json:"warnings,omitempty"`
}

type Common struct {
	PgBinPath     string `mapstructure:"pg_bin_path" yaml:"pg_bin_path,omitempty" json:"pg_bin_path,omitempty"`
	TempDirectory string `mapstructure:"tmp_dir" yaml:"tmp_dir,omitempty" json:"tmp_dir,omitempty"`
}

type StorageConfig struct {
	Type      string            `mapstructure:"type" yaml:"type" json:"type,omitempty"`
	S3        *s3.Config        `mapstructure:"s3"  json:"s3,omitempty" yaml:"s3"`
	Directory *directory.Config `mapstructure:"directory" json:"directory,omitempty" yaml:"directory"`
}

type LogConfig struct {
	Format string `mapstructure:"format" yaml:"format" json:"format,omitempty"`
	Level  string `mapstructure:"level" yaml:"level" json:"level,omitempty"`
}

type Dump struct {
	PgDumpOptions     pgdump.Options      `mapstructure:"pg_dump_options" yaml:"pg_dump_options" json:"pg_dump_options"`
	Transformation    []*Table            `mapstructure:"transformation" yaml:"transformation" json:"transformation,omitempty"`
	VirtualReferences []*VirtualReference `mapstructure:"virtual_references" yaml:"virtual_references" json:"virtual_references,omitempty"`
}

type Restore struct {
	PgRestoreOptions pgrestore.Options               `mapstructure:"pg_restore_options" yaml:"pg_restore_options" json:"pg_restore_options"`
	Scripts          map[string][]pgrestore.Script   `mapstructure:"scripts" yaml:"scripts" json:"scripts,omitempty"`
	ErrorExclusions  *DataRestorationErrorExclusions `mapstructure:"insert_error_exclusions" yaml:"insert_error_exclusions" json:"insert_error_exclusions,omitempty"`
}

type TablesDataRestorationErrorExclusions struct {
	Name        string   `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Schema      string   `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type GlobalDataRestorationErrorExclusions struct {
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type DataRestorationErrorExclusions struct {
	Tables []*TablesDataRestorationErrorExclusions `mapstructure:"tables" yaml:"tables" json:"tables,omitempty"`
	Global *GlobalDataRestorationErrorExclusions   `mapstructure:"global" yaml:"global" json:"global,omitempty"`
}

type TransformerSettings struct {
	NoValidateSchema           bool     `mapstructure:"no_validate_schema" yaml:"no_validate_schema" json:"no_validate_schema,omitempty"`
	ResolvedValidationWarnings []string `mapstructure:"resolved_validation_warnings" yaml:"resolved_validation_warnings" json:"resolved_validation_warnings,omitempty"`
}

type TransformerConfig struct {
	Name               string               `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	ApplyForReferences bool                 `mapstructure:"apply_for_references" yaml:"apply_for_references" json:"apply_for_references,omitempty"`
	Settings           *TransformerSettings `mapstructure:"settings,omitempty" yaml:"settings,omitempty" json:"settings,omitempty"`
	// Params - transformation parameters. It might be any type. If structure should be stored as raw json
	// This cannot be parsed with mapstructure due to uncontrollable lowercasing
	// https://github.com/spf13/viper/issues/373
	// Instead we have to use workaround and parse it manually
	//
	// Params attribute decoding is dummy. It is replaced in the runtime internal/utils/config/viper_workaround.go
	// But it is required to leave mapstruicture tag to avoid errors raised by viper and decoder setting
	// ErrorUnused = true. It was set in PR #177 (https://github.com/GreenmaskIO/greenmask/pull/177/files)
	Params toolkit.StaticParameters `mapstructure:"params" yaml:"-" json:"-"`
	// MetadataParams - encoded transformer parameters - uses only for storing into storage
	// TODO: You need to get rid of it by creating a separate structure for storing metadata in
	//   internal/db/postgres/storage/metadata_json.go
	// this is used only due to https://github.com/spf13/viper/issues/373
	MetadataParams map[string]any            `mapstructure:"-" yaml:"params,omitempty" json:"params,omitempty"`
	DynamicParams  toolkit.DynamicParameters `mapstructure:"dynamic_params" yaml:"dynamic_params" json:"dynamic_params,omitempty"`
	When           string                    `mapstructure:"when" yaml:"when" json:"when,omitempty"`
}

type Table struct {
	Schema              string               `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	Name                string               `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Query               string               `mapstructure:"query" yaml:"query" json:"query,omitempty"`
	ApplyForInherited   bool                 `mapstructure:"apply_for_inherited" yaml:"apply_for_inherited" json:"apply_for_inherited,omitempty"`
	Transformers        []*TransformerConfig `mapstructure:"transformers" yaml:"transformers" json:"transformers,omitempty"`
	ColumnsTypeOverride map[string]string    `mapstructure:"columns_type_override" yaml:"columns_type_override" json:"columns_type_override,omitempty"`
	SubsetConds         []string             `mapstructure:"subset_conds" yaml:"subset_conds" json:"subset_conds,omitempty"`
	When                string               `mapstructure:"when" yaml:"when" json:"when,omitempty"`
}

// DummyConfig - This is a dummy config to the viper workaround
// It is used to parse the transformation parameters manually only avoiding parsing other pars of the config
// The reason why is there https://github.com/GreenmaskIO/greenmask/discussions/85
type DummyConfig struct {
	Dump struct {
		Transformation []struct {
			Transformers []struct {
				Params map[string]interface{} `yaml:"params" json:"params"`
			} `yaml:"transformers" json:"transformers"`
		} `yaml:"transformation" json:"transformation"`
	} `yaml:"dump" json:"dump"`
}
