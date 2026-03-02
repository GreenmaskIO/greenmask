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
	"sync"
)

var (
	cfg  *Config
	once sync.Once
)

func NewConfig() *Config {
	once.Do(
		func() {
			cfg = &Config{
				Log:     NewLog(),
				Common:  NewCommon(),
				Storage: NewStorageConfig(),
				// TODO: Consider how to forward two dependencies
				//	dump and restore cfg interfaces.
				Dump:    NewDump(),
				Restore: NewRestore(),
			}
		},
	)
	return cfg
}

type Config struct {
	Engine   string        `mapstructure:"engine" yaml:"engine" json:"engine"`
	Common   Common        `mapstructure:"common" yaml:"common" json:"common"`
	Log      Log           `mapstructure:"log" yaml:"log" json:"log"`
	Storage  StorageConfig `mapstructure:"storage" yaml:"storage" json:"storage"`
	Dump     Dump          `mapstructure:"dump" yaml:"dump" json:"dump"`
	Validate Validate      `mapstructure:"validate" yaml:"validate" json:"validate"`
	Restore  Restore       `mapstructure:"restore" yaml:"restore" json:"restore"`
}
