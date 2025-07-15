package cmd

import (
	"github.com/greenmaskio/greenmask/v1/internal/config"
	mysqlconfig "github.com/greenmaskio/greenmask/v1/internal/mysql/config"
)

const (
	latestDumpName = "latest"
)

var (
	Config = config.NewConfig(mysqlconfig.NewDumpOptions())
)
