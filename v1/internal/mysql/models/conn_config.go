package models

import (
	"fmt"
	"time"
)

type ConnConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Timeout  time.Duration
	// TODO: Add TLS setting.
}

func (d *ConnConfig) Address() string {
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

func (d *ConnConfig) URI() (string, error) {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", d.User, d.Password, d.Host, d.Port, d.Database), nil
}
