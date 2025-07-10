package models

import "time"

type ConnConfig struct {
	Address  string
	User     string
	Password string
	DbName   string
	Timeout  time.Duration
	// TODO: Add TLS setting.
}
