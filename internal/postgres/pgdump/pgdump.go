package pgdump

import (
	"context"
	"fmt"
	"os/exec"
)

type PgDump struct {
	binPath        string
	defaultOptions map[string]string
}

func NewPgDump(binPath string) *PgDump {
	return &PgDump{
		binPath: binPath,
		defaultOptions: map[string]string{
			"format": "c",
		},
	}
}

func (pd *PgDump) Run(ctx context.Context, options *Options) error {
	execOptions, err := options.GetExec()
	if err != nil {
		return fmt.Errorf("cannot get options: %w", err)
	}
	cmd := exec.CommandContext(ctx, pd.binPath, execOptions...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_dump runtime error: %w", err)
	}
	return nil
}

type Options struct {
	// General options:
	FileName        string // --file=FILENAME
	Format          string // --lock-wait-timeout=TIMEOUT
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
	Port       uint32 // --port=PORT
	UserName   string // --username=NAME
	NoPassword string // --no-password
	Password   string // --password
	Role       string // --role=ROLENAME
}

func (s *Options) GetExec() ([]string, error) {
	return nil, nil
}
