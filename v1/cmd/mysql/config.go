package mysql

import (
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql"
)

const (
	latestDumpName = "latest"
)

var (
	Config = config.NewConfig(mysql.NewDumpOptions())
)
