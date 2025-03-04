package mysql

import "fmt"

type DumpOptions struct {
	// Connection details
	User     string `mapstructure:"user"`     // MySQL username
	Password string `mapstructure:"password"` // MySQL password
	Host     string `mapstructure:"host"`     // MySQL server hostname or IP
	Port     int    `mapstructure:"port"`     // MySQL server port, default is 3306
	Database string `mapstructure:"database"` // Name of the database to dump

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

func NewDumpOptions() *DumpOptions {
	return &DumpOptions{}
}

// GetParams generates a slice of command-line arguments for mysqldump based on DumpOptions.
func (d *DumpOptions) GetParams() ([]string, error) {
	var args []string

	// Connection options
	if d.User != "" {
		args = append(args, "-u", d.User)
	}
	if d.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", d.Password))
	}
	if d.Host != "" {
		args = append(args, "-h", d.Host)
	}
	if d.Port != 0 {
		args = append(args, fmt.Sprintf("-P%d", d.Port))
	}

	// General dump options
	if d.NoCreateInfo {
		args = append(args, "--no-create-info")
	}
	if d.NoData {
		args = append(args, "--no-data")
	}
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
	if d.Quick {
		args = append(args, "--quick")
	}
	if d.LockTables {
		args = append(args, "--lock-tables")
	}
	if d.NoTablespaces {
		args = append(args, "--no-tablespaces")
	}

	// Specify the database
	if d.Database != "" {
		args = append(args, d.Database)
	}

	return args, nil
}

func (d *DumpOptions) GetConnURI() (string, error) {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", d.User, d.Password, d.Host, d.Port, d.Database), nil
}
