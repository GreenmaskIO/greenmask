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

const (
	defaultTmpDirectoryPath = "/tmp"
)

type Common struct {
	BinPath       string `mapstructure:"bin_path" yaml:"bin_path,omitempty" json:"bin_path,omitempty"`
	TempDirectory string `mapstructure:"tmp_dir" yaml:"tmp_dir,omitempty" json:"tmp_dir,omitempty"`
}

func NewCommon() Common {
	return Common{
		TempDirectory: defaultTmpDirectoryPath,
	}
}
