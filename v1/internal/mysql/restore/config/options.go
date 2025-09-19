package config

import (
	"github.com/greenmaskio/greenmask/v1/internal/mysql/config"
)

type RestoreOptions struct {
	config.ConnectionOpts `mapstructure:",squash"`
	Verbose               bool `mapstructure:"verbose"`
}

func (r *RestoreOptions) SchemaRestoreParams() ([]string, error) {
	params := r.ConnectionOpts.Params()
	if r.Verbose {
		params = append(params, "--verbose")
	}
	return params, nil
}
