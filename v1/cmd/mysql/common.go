package mysql

import (
	"github.com/greenmaskio/greenmask/internal/db/mysql"
	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
)

const (
	latestDumpName = "latest"
)

var (
	Config = pgDomains.NewConfig(mysql.NewDumpOptions(), nil)
)
