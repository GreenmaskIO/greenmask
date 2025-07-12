package cmd

import (
	"github.com/greenmaskio/greenmask/v1/internal/config"
	config2 "github.com/greenmaskio/greenmask/v1/internal/mysql/config"
)

const (
	latestDumpName = "latest"
)

var (
	Config = config.NewConfig(config2.NewDumpOptions())
)
