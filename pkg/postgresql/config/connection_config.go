package config

import (
	"fmt"
	"os"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	"github.com/greenmaskio/greenmask/pkg/postgresql/models"
)

type ConnectionOpts struct {
	User             string `mapstructure:"user"               json:"user,omitempty"`
	Password         string `mapstructure:"password"           json:"password,omitempty"`
	Host             string `mapstructure:"host"               json:"host,omitempty"`
	Port             int    `mapstructure:"port"               json:"port,omitempty"`
	Socket           string `mapstructure:"socket"             json:"socket,omitempty"`
	ConnectDatabase  string `mapstructure:"connect-database"   json:"connect_database,omitempty"`
	MaxAllowedPacket int    `mapstructure:"max-allowed-packet" json:"max_allowed_packet,omitempty"`

	// Connection options:
	URI             string `mapstructure:"uri"`
	ConnectDatabase string `mapstructure:"connect_database"`
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	User            string `mapstructure:"username"`
	Password        bool   `mapstructure:"password"`
	Role            string `mapstructure:"role"`
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

// ConnectionConfig builds a ready-to-use *config.ConnConfig from these coordinates
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
