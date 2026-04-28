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

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/models"
)

const (
	DefaultMaxAllowedPacket = 0
)

// ConnectionOpts holds the coordinates needed to reach a MySQL server:
// who to connect as, where to connect, and transport-level limits.
// SSL policy is deliberately absent — callers pass a resolved commonconfig.SSLOpts
// to Params and ConnectionConfig so the merge logic stays at the operation layer.
type ConnectionOpts struct {
	User             string `mapstructure:"user"               json:"user,omitempty"`
	Password         string `mapstructure:"password"           json:"password,omitempty"`
	Host             string `mapstructure:"host"               json:"host,omitempty"`
	Port             int    `mapstructure:"port"               json:"port,omitempty"`
	Socket           string `mapstructure:"socket"             json:"socket,omitempty"`
	ConnectDatabase  string `mapstructure:"connect-database"   json:"connect_database,omitempty"`
	MaxAllowedPacket int    `mapstructure:"max-allowed-packet" json:"max_allowed_packet,omitempty"`
}

func (d *ConnectionOpts) Env() ([]string, error) {
	env := []string{
		"MYSQL_PWD=" + d.Password,
	}
	if d.Host != "" {
		env = append(env, "MYSQL_HOST="+d.Host)
	}
	if d.Port != 0 {
		env = append(env, fmt.Sprintf("MYSQL_PORT=%d", d.Port))
	}
	return append(env, os.Environ()...), nil
}

// Params builds CLI flags for mysqldump/mysql subprocesses, including SSL flags
// derived from the already-merged ssl argument.
func (d *ConnectionOpts) Params(ssl commonconfig.SSLOpts) []string {
	var args []string

	if d.User != "" {
		args = append(args, "--user", d.User)
	}

	// Socket takes precedence over host/port for CLI.
	if d.Socket != "" {
		args = append(args, "--socket", d.Socket)
	} else {
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
	}

	// Always emit ssl_mode when set so the CLI tool doesn't silently default to PREFERRED.
	if ssl.Mode != "" {
		args = append(args, "--ssl-mode", string(ssl.Mode))
	}
	if ssl.CA != "" {
		args = append(args, "--ssl-ca", ssl.CA)
	}
	if ssl.CAPath != "" {
		args = append(args, "--ssl-capath", ssl.CAPath)
	}
	if ssl.Cert != "" {
		args = append(args, "--ssl-cert", ssl.Cert)
	}
	if ssl.Key != "" {
		args = append(args, "--ssl-key", ssl.Key)
	}
	if ssl.Cipher != "" {
		args = append(args, "--ssl-cipher", ssl.Cipher)
	}
	if ssl.TLSVersion != "" {
		args = append(args, "--tls-version", ssl.TLSVersion)
	}
	if ssl.TLSCipherSuites != "" {
		args = append(args, "--tls-ciphersuites", ssl.TLSCipherSuites)
	}

	return args
}

// ConnectionConfig builds a ready-to-use *models.ConnConfig from these coordinates
// and the ssl argument from the common options section.
func (d *ConnectionOpts) ConnectionConfig(ssl commonconfig.SSLOpts) (*models.ConnConfig, error) {
	host := d.Host
	if d.Socket == "" && host == "localhost" {
		host = "127.0.0.1"
	}

	tlsCfg, err := ssl.TLSConfig(host)
	if err != nil {
		return nil, fmt.Errorf("build TLS config: %w", err)
	}

	return &models.ConnConfig{
		User:             d.User,
		Password:         d.Password,
		Host:             host,
		Port:             d.Port,
		Socket:           d.Socket,
		Database:         d.ConnectDatabase,
		MaxAllowedPacket: d.MaxAllowedPacket,
		SSLMode:          ssl.Mode,
		TLSConfig:        tlsCfg,
	}, nil
}
