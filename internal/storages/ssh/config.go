// Copyright 2026 Greenmask
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

package ssh

import "fmt"

const defaultPort = 22

type Config struct {
	Host           string `mapstructure:"host"`             // required
	Port           int    `mapstructure:"port"`             // default 22
	User           string `mapstructure:"user"`             // required
	Password       string `mapstructure:"password"`         // auth: password
	PrivateKeyPath string `mapstructure:"private_key_path"` // auth: private key
	Prefix         string `mapstructure:"prefix"`           // remote root path
}

func NewConfig() *Config {
	return &Config{Port: defaultPort}
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	if c.Password == "" && c.PrivateKeyPath == "" {
		return fmt.Errorf("one of password or private_key_path is required")
	}
	if c.Port <= 0 {
		c.Port = defaultPort
	}
	return nil
}
