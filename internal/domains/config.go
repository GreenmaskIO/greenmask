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
	Common             Common                          `mapstructure:"common" yaml:"common"`
	Log                LogConfig                       `mapstructure:"log" yaml:"log"`
	Storage            StorageConfig                   `mapstructure:"storage" yaml:"storage"`
	Dump               Dump                            `mapstructure:"dump" yaml:"dump"`
	Validate           Validate                        `mapstructure:"validate" yaml:"validate"`
	Restore            Restore                         `mapstructure:"restore" yaml:"restore"`
	CustomTransformers []*custom.TransformerDefinition `mapstructure:"custom_transformers" yaml:"custom_transformers"`
}

type Validate struct {
	Tables           []string `mapstructure:"tables" yaml:"tables"`
	Data             bool     `mapstructure:"data" yaml:"data"`
	Diff             bool     `mapstructure:"diff" yaml:"diff"`
	RowsLimit        uint64   `mapstructure:"rows_limit" yaml:"rows_limit"`
	ResolvedWarnings []string `mapstructure:"resolved_warnings" yaml:"resolved_warnings"`
	Format           string   `mapstructure:"format" yaml:"format"`
}

type Common struct {
	PgBinPath     string `mapstructure:"pg_bin_path" yaml:"pg_bin_path,omitempty"`
	TempDirectory string `mapstructure:"tmp_dir" yaml:"tmp_dir,omitempty"`
}

type StorageConfig struct {
	S3        *s3.Config        `mapstructure:"s3"`
	Directory *directory.Config `mapstructure:"directory"`
}

type LogConfig struct {
	Format string `mapstructure:"format" yaml:"format"`
	Level  string `mapstructure:"level" yaml:"level"`
}

type Dump struct {
	PgDumpOptions  pgdump.Options `mapstructure:"pg_dump_options" yaml:"pg_dump_options"`
	Transformation []*Table       `mapstructure:"transformation" yaml:"transformation"`
}

type Restore struct {
	PgRestoreOptions pgrestore.Options             `mapstructure:"pg_restore_options" yaml:"pgRestoreOptions"`
	Scripts          map[string][]pgrestore.Script `mapstructure:"scripts" yaml:"scripts"`
}

type TransformerSettings struct {
	NoValidateSchema           bool     `mapstructure:"no_validate_schema" yaml:"no_validate_schema"`
	ResolvedValidationWarnings []string `mapstructure:"resolved_validation_warnings" yaml:"resolved_validation_warnings"`
}

type TransformerConfig struct {
	Name     string                         `mapstructure:"name" yaml:"name"`
	Settings TransformerSettings            `mapstructure:"settings" yaml:"settings"`
	Params   map[string]toolkit.ParamsValue `mapstructure:"params" yaml:"params"`
}

type Table struct {
	Schema              string               `mapstructure:"schema" yaml:"schema"`
	Name                string               `mapstructure:"name" yaml:"name"`
	Query               string               `mapstructure:"query" yaml:"query"`
	ApplyForInherited   bool                 `mapstructure:"apply_for_inherited" yaml:"apply_for_inherited"`
	Transformers        []*TransformerConfig `mapstructure:"transformers" yaml:"transformers"`
	ColumnsTypeOverride map[string]string    `mapstructure:"columns_type_override" yaml:"columns_type_override"`
}
