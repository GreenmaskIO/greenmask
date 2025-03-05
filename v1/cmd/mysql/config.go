package mysql

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql"
)

const (
	latestDumpName = "latest"
)

var (
	Config = config.NewConfig(mysql.NewDumpOptions())
)
