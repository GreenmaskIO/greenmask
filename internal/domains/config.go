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

func NewConfig() *Config {
	once.Do(
		func() {
			Cfg = &Config{
				Storage: StorageConfig{
					S3: s3.NewConfig(),
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
	RowsLimit        uint64   `mapstructure:"rows_limit" yaml:"rows_limit" json:"rows_limit,omitempty"`
	ResolvedWarnings []string `mapstructure:"resolved_warnings" yaml:"resolved_warnings" json:"resolved_warnings,omitempty"`
	Format           string   `mapstructure:"format" yaml:"format" json:"format,omitempty"`
}

type Common struct {
	PgBinPath     string `mapstructure:"pg_bin_path" yaml:"pg_bin_path,omitempty" json:"pg_bin_path,omitempty"`
	TempDirectory string `mapstructure:"tmp_dir" yaml:"tmp_dir,omitempty" json:"tmp_dir,omitempty"`
}

type StorageConfig struct {
	S3        *s3.Config        `mapstructure:"s3"  json:"s3,omitempty" yaml:"s3"`
	Directory *directory.Config `mapstructure:"directory" json:"directory,omitempty" yaml:"directory"`
}

type LogConfig struct {
	Format string `mapstructure:"format" yaml:"format" json:"format,omitempty"`
	Level  string `mapstructure:"level" yaml:"level" json:"level,omitempty"`
}

type Dump struct {
	PgDumpOptions  pgdump.Options `mapstructure:"pg_dump_options" yaml:"pg_dump_options" json:"pg_dump_options"`
	Transformation []*Table       `mapstructure:"transformation" yaml:"transformation" json:"transformation,omitempty"`
}

type Restore struct {
	PgRestoreOptions pgrestore.Options             `mapstructure:"pg_restore_options" yaml:"pg_restore_options" json:"pg_restore_options"`
	Scripts          map[string][]pgrestore.Script `mapstructure:"scripts" yaml:"scripts" json:"scripts,omitempty"`
}

type TransformerSettings struct {
	NoValidateSchema           bool     `mapstructure:"no_validate_schema" yaml:"no_validate_schema" json:"no_validate_schema,omitempty"`
	ResolvedValidationWarnings []string `mapstructure:"resolved_validation_warnings" yaml:"resolved_validation_warnings" json:"resolved_validation_warnings,omitempty"`
}

type TransformerConfig struct {
	Name          string                `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Settings      *TransformerSettings  `mapstructure:"settings,omitempty" yaml:"settings,omitempty" json:"settings,omitempty"`
	Params        toolkit.Params        `mapstructure:"params" yaml:"params" json:"params,omitempty"`
	DynamicParams toolkit.DynamicParams `mapstructure:"dynamic_params" yaml:"dynamic_params" json:"dynamic_params,omitempty"`
}

type Table struct {
	Schema              string               `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	Name                string               `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Query               string               `mapstructure:"query" yaml:"query" json:"query,omitempty"`
	ApplyForInherited   bool                 `mapstructure:"apply_for_inherited" yaml:"apply_for_inherited" json:"apply_for_inherited,omitempty"`
	Transformers        []*TransformerConfig `mapstructure:"transformers" yaml:"transformers" json:"transformers,omitempty"`
	ColumnsTypeOverride map[string]string    `mapstructure:"columns_type_override" yaml:"columns_type_override" json:"columns_type_override,omitempty"`
}
