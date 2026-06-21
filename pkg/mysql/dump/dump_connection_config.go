package dump

import (
	"github.com/greenmaskio/greenmask/pkg/config"
)

// DumpConnectionConfig is the MySQL-specific ConnectionConfigurer.
// It bundles the shared dump options with MySQL-specific connection parameters.
type DumpConnectionConfig struct {
	Common config.CommonDumpOptions
	MySQL  config.MysqlDumpConfig
	// ConnectionPoolSize is the number of snapshot-synchronized worker
	// connections the dump session opens. It is derived from the dump jobs count
	// by ConnectionConfigurerBuilder and is always >= 1.
	ConnectionPoolSize int
}

func (c *DumpConnectionConfig) ConnectionConfig() any {
	return c
}

// The methods below expose the connection attributes the mysqldump-backed
// schema dumper needs, transformed from this config. They let the schema dump
// factory derive the CLI environment and parameters at Dump time via a small
// interface, without importing this package (which would form an import cycle).

// MysqldumpEnv returns the process environment for a mysqldump invocation.
func (c *DumpConnectionConfig) MysqldumpEnv() ([]string, error) {
	return c.MySQL.Env()
}

// MysqldumpConnParams returns the mysqldump connection/auth CLI flags, including
// the SSL flags derived from the shared dump options.
func (c *DumpConnectionConfig) MysqldumpConnParams() []string {
	return c.MySQL.Params(c.Common.SSL)
}

// MysqldumpVendorOptions returns the user-specified arbitrary mysqldump options.
func (c *DumpConnectionConfig) MysqldumpVendorOptions() []string {
	return c.MySQL.VendorOptions
}
