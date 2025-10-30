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
	"fmt"
	"os"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/models"
)

type ConnectionOpts struct {
	// Connection details
	User            string `mapstructure:"user"`     // MySQL username
	Password        string `mapstructure:"password"` // MySQL password
	Host            string `mapstructure:"host"`     // MySQL server hostname or IP
	Port            int    `mapstructure:"port"`     // MySQL server port, default is 3306
	ConnectDatabase string `mapstructure:"connect-database"`
}

func (d *ConnectionOpts) Env() ([]string, error) {
	env := []string{
		"MYSQL_PWD=" + d.Password,
	}

	// Optional connection-related environment variables
	if d.Host != "" {
		env = append(env, "MYSQL_HOST="+d.Host)
	}
	if d.Port != 0 {
		env = append(env, fmt.Sprintf("MYSQL_PORT=%d", d.Port))
	}

	// Inherit parent environment securely
	return append(env, os.Environ()...), nil
}

func (d *ConnectionOpts) Params() []string {
	var args []string
	//// Connection options
	if d.User != "" {
		args = append(args, "--user", d.User)
	}
	if d.Port != 0 {
		args = append(args, "--port", fmt.Sprintf("%d", d.Port))
	}
	if d.Host != "" {
		host := d.Host
		if host == "localhost" {
			host = "127.0.0.1"
		}
		args = append(args, "--host", host)
	}
	return args
}

func (d *ConnectionOpts) ConnectionConfig() (interfaces.ConnectionConfigurator, error) {
	return &models.ConnConfig{
		User:     d.User,
		Password: d.Password,
		Host:     d.Host,
		Port:     d.Port,
		Database: d.ConnectDatabase,
	}, nil
}
