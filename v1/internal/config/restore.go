package config

import (
	pgconfig "github.com/greenmaskio/greenmask/v1/internal/pg/restore/config"

	mysqlconfig "github.com/greenmaskio/greenmask/v1/internal/mysql/restore/config"
)

type TablesDataRestorationErrorExclusions struct {
	Name        string   `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Schema      string   `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type GlobalDataRestorationErrorExclusions struct {
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type DataRestorationErrorExclusions struct {
	Tables []TablesDataRestorationErrorExclusions `mapstructure:"tables" yaml:"tables" json:"tables,omitempty"`
	Global GlobalDataRestorationErrorExclusions   `mapstructure:"global" yaml:"global" json:"global,omitempty"`
}

type Script struct {
	Name      string   `mapstructure:"name"`
	When      string   `mapstructure:"when"`
	Query     string   `mapstructure:"query"`
	QueryFile string   `mapstructure:"query_file"`
	Command   []string `mapstructure:"command"`
}

type MysqlRestoreConfig struct {
	Options mysqlconfig.RestoreOptions `mapstructure:"options" yaml:"options" json:"options"`
}

type PostgresqlRestoreConfig struct {
	Options pgconfig.RestoreOptions `mapstructure:"options" yaml:"options" json:"options"`
}

type Restore struct {
	Options          Options                        `mapstructure:"options" yaml:"options" json:"options"`
	MysqlConfig      MysqlRestoreConfig             `mapstructure:"mysql" yaml:"mysql"`
	PostgresqlConfig PostgresqlRestoreConfig        `mapstructure:"postgresql" yaml:"postgresql"`
	Scripts          map[string][]Script            `mapstructure:"scripts" yaml:"scripts" json:"scripts,omitempty"`
	ErrorExclusions  DataRestorationErrorExclusions `mapstructure:"insert_error_exclusions" yaml:"insert_error_exclusions" json:"insert_error_exclusions,omitempty"`
}

func NewRestore() Restore {
	return Restore{}
}
