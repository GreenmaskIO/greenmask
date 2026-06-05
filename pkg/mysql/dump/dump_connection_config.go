package dump

import "github.com/greenmaskio/greenmask/pkg/config"

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
