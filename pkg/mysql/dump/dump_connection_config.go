package dump

import "github.com/greenmaskio/greenmask/pkg/config"

// DumpConnectionConfig is the MySQL-specific ConnectionConfigurer.
// It bundles the shared dump options with MySQL-specific connection parameters.
type DumpConnectionConfig struct {
	Common config.CommonDumpOptions
	MySQL  config.MysqlDumpConfig
}

func (c *DumpConnectionConfig) ConnectionConfig() any {
	return c
}
