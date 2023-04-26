package pgdump

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/rs/zerolog/log"
)

const pgDumpExecutable = "pg_dump"

type PgDump struct {
	BinPath string
}

func NewPgDump(binPath string) *PgDump {
	return &PgDump{
		BinPath: binPath,
	}
}

func (pd *PgDump) Run(ctx context.Context, options *Options) error {
	log.Debug().Msgf("pg_dump: %s %s\n", path.Join(pd.BinPath, pgDumpExecutable), strings.Join(options.GetParams(), " "))
	execOptions := options.GetParams()
	cmd := exec.CommandContext(ctx, path.Join(pd.BinPath, pgDumpExecutable), execOptions...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_dump runtime error: %w", err)
	}

	return nil
}

type Options struct {
	// General options:
	FileName        string `mapstructure:"file"` // --file=FILENAME
	Format          string // Supports only directory format
	Jobs            string `mapstructure:"jobs"`
	Verbose         bool   `mapstructure:"verbose"`
	Compression     int    `mapstructure:"compress"`
	LockWaitTimeout int    `mapstructure:"lock-wait-timeout"`
	NoSync          bool   `mapstructure:"no-sync"`

	DataOnly                   bool     `mapstructure:"data-only"`
	Blobs                      bool     `mapstructure:"blobs"`
	Clean                      bool     `mapstructure:"clean"`
	Create                     bool     `mapstructure:"create"`
	Extension                  []string `mapstructure:"extension"`
	Encoding                   string   `mapstructure:"encoding"`
	Schema                     []string `mapstructure:"schema"`
	ExcludeSchema              []string `mapstructure:"exclude-schema"`
	NoOwner                    bool     `mapstructure:"no-owner"`
	SchemaOnly                 bool     `mapstructure:"schema-only"`
	SuperUser                  string   `mapstructure:"superuser"`
	Table                      []string `mapstructure:"table"`
	ExcludeTable               []string `mapstructure:"exclude-table"`
	NoPrivileges               bool     `mapstructure:"no-privileges"`
	DisableDollarQuoting       bool     `mapstructure:"disable-dollar-quoting"`
	DisableTriggers            bool     `mapstructure:"disable-triggers"`
	EnableRowSecurity          bool     `mapstructure:"enable-row-security"`
	ExcludeTableData           []string `mapstructure:"exclude-table-data"`
	ExtraFloatDigits           string   `mapstructure:"extra-float-digits"`
	IfExists                   bool     `mapstructure:"if-exists"`
	IncludeForeignData         []string `mapstructure:"include-foreign-data"`
	LoadViaPartitionRoot       bool     `mapstructure:"load-via-partition-root"`
	NoComments                 bool     `mapstructure:"no-comments"`
	NoPublications             bool     `mapstructure:"no-publications"`
	NoSecurityLabels           bool     `mapstructure:"no-security-labels"`
	NoSubscriptions            bool     `mapstructure:"no-subscriptions"`
	NoSynchronizedSnapshots    bool     `mapstructure:"no-synchronized-snapshots"`
	NoTableSpaces              bool     `mapstructure:"no-tablespaces"`
	NoToastCompression         bool     `mapstructure:"no-toast-compression"`
	NoUnloggedTableData        bool     `mapstructure:"no-unlogged-table-data"`
	OnConflictDoNothing        bool     `mapstructure:"on-conflict-do-nothing"`
	QuoteAllIdentifiers        bool     `mapstructure:"quote-all-identifiers"`
	Section                    string   `mapstructure:"section"`
	SerializableDeferrable     string   `mapstructure:"serializable-deferrable"`
	Snapshot                   string   `mapstructure:"snapshot"`
	StrictNames                string   `mapstructure:"strict-names"`
	UseSetSessionAuthorization bool     `mapstructure:"use-set-session-authorization"`

	// Specifies the name of the database to connect to. This is equivalent to specifying dbname as the first
	// non-option argument on the command line. The dbname can be a connection string. If so, connection string
	// parameters will override any conflicting command line options.
	DbName     string `mapstructure:"dbname"`
	Host       string `mapstructure:"host"`
	Port       int    `mapstructure:"port"`
	UserName   string `mapstructure:"username"`
	NoPassword string `mapstructure:"no-password"`
	Password   string `mapstructure:"password"`
	Role       string `mapstructure:"role"`
}

func (o *Options) GetPgDSN() (string, error) {
	//return "host=localhost port=5432 user=postgres dbname=postgres", nil
	if strings.Contains(o.DbName, "=") {
		return o.DbName, nil
	}
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s", o.Host, o.Port, o.UserName, o.DbName), nil
}

func (o *Options) GetParams() []string {
	args := []string{
		"--username", o.UserName,
		"--dbname", o.DbName,
		"--username", o.UserName,
		"--format", o.Format,
	}
	if o.SchemaOnly {
		args = append(args, "--schema-only")
	}
	if o.FileName != "" {
		args = append(args, "--file", o.FileName)
	}
	if o.Verbose {
		args = append(args, "--verbose")
	}
	if o.Section != "" {
		args = append(args, "--section", o.Section)
	}
	return args
}
