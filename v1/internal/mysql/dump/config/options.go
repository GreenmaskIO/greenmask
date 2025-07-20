package config

import (
	"fmt"
	"os"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/models"
)

type DumpOptions struct {
	config.ConnectionOpts
	// General dump options
	NoCreateInfo      bool `mapstructure:"no-create-info"`     // Exclude CREATE TABLE statements (--no-create-info)
	NoData            bool `mapstructure:"no-data"`            // Exclude data from dump (--no-data)
	AddDropTable      bool `mapstructure:"add-drop-table"`     // Include DROP TABLE statements (--add-drop-table)
	Compact           bool `mapstructure:"compact"`            // Reduce dump size with minimal comments (--compact)
	SkipComments      bool `mapstructure:"skip-comments"`      // Do not include comments in dump (--skip-comments)
	SingleTransaction bool `mapstructure:"single-transaction"` // Use a single transaction for the dump (--single-transaction)
	Quick             bool `mapstructure:"quick"`              // Fetch rows one at a time (--quick)
	LockTables        bool `mapstructure:"lock-tables"`        // Lock all tables during dump (--lock-tables)

	// Tablespace and metadata options
	NoTablespaces bool `mapstructure:"no-tablespaces"` // Exclude tablespace information (--no-tablespaces)
}

func (d *DumpOptions) GetIncludedTables() []string {
	return nil
}

func (d *DumpOptions) GetExcludedTables() []string {
	return nil
}

func (d *DumpOptions) GetExcludedSchemas() []string {
	return nil
}

func (d *DumpOptions) GetIncludedSchemas() []string {
	return nil
}

func (d *DumpOptions) Env() ([]string, error) {
	env := []string{
		"MYSQL_PWD=" + d.Password,
	}

	// Optional connection-related environment variables
	if d.Host != "" {
		env = append(env, "MYSQL_HOST="+d.Host)
	}
	if d.Port != 0 {
		env = append(env, fmt.Sprintf("MYSQL_PORT=%d", d.Port))
	}

	// Inherit parent environment securely
	return append(env, os.Environ()...), nil
}

func (d *DumpOptions) SchemaDumpParams() ([]string, error) {
	args := d.ConnectionOpts.Params()
	args = append(args, "--no-data")
	if d.AddDropTable {
		args = append(args, "--add-drop-table")
	}
	if d.Compact {
		args = append(args, "--compact")
	}
	if d.SkipComments {
		args = append(args, "--skip-comments")
	}
	if d.SingleTransaction {
		args = append(args, "--single-transaction")
	}
	if d.LockTables {
		args = append(args, "--lock-tables")
	}
	if d.NoTablespaces {
		args = append(args, "--no-tablespaces")
	}
	return args, nil
}

func (d *DumpOptions) Get(key string) (any, error) {
	panic("not implemented")
}

func (d *DumpOptions) ConnectionConfig() (interfaces.ConnectionConfigurator, error) {
	database := d.ConnectDatabase
	if database == "" {
		database = d.Databases[0]
	}
	return &models.ConnConfig{
		User:     d.User,
		Password: d.Password,
		Host:     d.Host,
		Port:     d.Port,
		Database: database,
	}, nil
}
