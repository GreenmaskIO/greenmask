package config

import (
	"github.com/greenmaskio/greenmask/v1/internal/mysql/config"
)

type RestoreOptions struct {
	config.ConnectionOpts
}

func (r *RestoreOptions) SchemaRestoreParams() ([]string, error) {
	return r.ConnectionOpts.Params(), nil
}
