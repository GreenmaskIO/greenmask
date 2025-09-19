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
