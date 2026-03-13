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
	"github.com/greenmaskio/greenmask/pkg/mysql/config"
)

const DefaultMaxFetchWarnings = 10

type RestoreOptions struct {
	config.ConnectionOpts `mapstructure:",squash"`
	Verbose               bool `mapstructure:"verbose" yaml:"verbose" json:"verbose"`
	PrintWarnings         bool `mapstructure:"print-warnings" yaml:"print-warnings" json:"print_warnings"`
	// MaxFetchWarnings - the maximum number of warnings to fetch and print. If 0, all warnings are printed.
	MaxFetchWarnings int `mapstructure:"max-fetch-warnings" yaml:"max-fetch-warnings" json:"max_fetch_warnings"`
}

func NewRestoreOptions() RestoreOptions {
	return RestoreOptions{
		PrintWarnings:    false,
		MaxFetchWarnings: DefaultMaxFetchWarnings,
	}
}

func (r *RestoreOptions) Validate() error {
	return nil
}

func (r *RestoreOptions) SchemaRestoreParams() ([]string, error) {
	params := r.Params()
	if r.Verbose {
		params = append(params, "--verbose")
	}
	return params, nil
}
