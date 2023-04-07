package pgdump

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"os/exec"
	"path"
	"strings"
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
	FileName        string // --file=FILENAME
	Format          string // --format=format p|c|d|t
	Jobs            string // --jobs=NUM
	Verbose         bool   // --verbose
	Compression     int    // --compress=0-9
	LockWaitTimeout int    // --lock-wait-timeout=TIMEOUT
	NoSync          bool   // --no-sync

	DataOnly                   bool   // --data-only
	Blobs                      bool   // --blobs | --no-blobs
	Clean                      bool   // --clean
	Create                     bool   // --create
	Extension                  string // --extension=PATTERN
	Encoding                   string // --encoding=ENCODING
	Schema                     string // --schema=PATTERN
	ExcludeSchema              string // --exclude-schema=PATTERN
	NoOwner                    bool   // --no-owner
	SchemaOnly                 bool   // --schema-only
	SuperUser                  string // --superuser=NAME
	Table                      string // --table=PATTERN
	ExcludeTable               string // --exclude-table=PATTERN
	NoPrivileges               bool   // --exclude-table=PATTERN
	ColumnInserts              bool   // --column-inserts
	BinaryUpgrade              bool   // --binary-upgrade
	DisableDollarQuoting       bool   // --disable-dollar-quoting
	DisableTriggers            bool   // --disable-triggers
	EnableRowSecurity          bool   // --enable-row-security
	ExcludeTableData           bool   // --exclude-table-data=PATTERN
	ExtraFloatDigits           string // --extra-float-digits=NUM
	IfExists                   bool   // --extra-float-digits=NUM
	IncludeForeignData         string // --include-foreign-data=PATTERN
	Inserts                    bool   // --inserts
	LoadViaPartitionRoot       bool   // --load-via-partition-root
	NoComments                 bool   // --no-comments
	NoPublications             bool   // --no-comments
	NoSecurityLabels           bool   // --no-comments
	NoSubscriptions            bool   // --no-subscriptions
	NoSynchronizedSnapshots    bool   // --no-synchronized-snapshots
	NoTableSpaces              bool   // --no-tablespaces
	NoToastCompression         bool   // --no-toast-compression
	NoUnloggedTableData        bool   // --no-unlogged-table-data
	OnConflictDoNothing        bool   // --on-conflict-do-nothing
	QuoteAllIdentifiers        bool   // --quote-all-identifiers
	RowsPerInsert              int    // --rows-per-insert=NROWS
	Section                    string // --section=SECTION
	SerializableDeferrable     string // --serializable-deferrable
	Snapshot                   string // --snapshot=SNAPSHOT
	StrictNames                string // --strict-names
	UseSetSessionAuthorization bool   // --use-set-session-authorization

	DbName     string // --dbname=DBNAME
	Host       string // --host=HOSTNAME
	Port       int    // --port=PORT
	UserName   string // --username=NAME
	NoPassword string // --no-password
	Password   string // --password
	Role       string // --role=ROLENAME
}

func (*Options) GetPgDSN() (string, error) {
	return "host=localhost port=5432 user=postgres dbname=postgres", nil
}

func (o *Options) GetParams() []string {
	args := []string{
		"--username", o.UserName,
		"--dbname", o.DbName,
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
