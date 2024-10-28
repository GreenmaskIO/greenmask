// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgdump

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/utils/cmd_runner"
)

const pgDumpExecutable = "pg_dump"

const pgDefaultPort = 5432

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
	return cmd_runner.Run(ctx, &log.Logger, path.Join(pd.BinPath, pgDumpExecutable), options.GetParams()...)
}

type Options struct {
	// General options:
	FileName        string `mapstructure:"file"` // --file=FILENAME
	Format          string // Supports only directory format
	Jobs            int    `mapstructure:"jobs"`
	Verbose         bool   `mapstructure:"verbose"`
	Compression     int    `mapstructure:"compress"`
	LockWaitTimeout int    `mapstructure:"lock-wait-timeout"`
	NoSync          bool   `mapstructure:"no-sync"`

	// Options controlling the output content
	DataOnly                   bool     `mapstructure:"data-only"`
	Blobs                      bool     `mapstructure:"blobs"`
	NoBlobs                    bool     `mapstructure:"no-blobs"`
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
	QuoteAllIdentifiers        bool     `mapstructure:"quote-all-identifiers"`
	Section                    string   `mapstructure:"section"`
	SerializableDeferrable     bool     `mapstructure:"serializable-deferrable"`
	Snapshot                   string   `mapstructure:"snapshot"`
	StrictNames                bool     `mapstructure:"strict-names"`
	UseSetSessionAuthorization bool     `mapstructure:"use-set-session-authorization"`

	// Custom options (not from pg_dump)
	// Use pgzip compression instead of gzip
	Pgzip bool `mapstructure:"pgzip"`

	// Connection options:
	DbName     string `mapstructure:"dbname"`
	Host       string `mapstructure:"host"`
	Port       int    `mapstructure:"port"`
	UserName   string `mapstructure:"username"`
	NoPassword bool   `mapstructure:"no-password"`
	Password   bool   `mapstructure:"password"`
	Role       string `mapstructure:"role"`
}

func (o *Options) GetPgDSN() (string, error) {
	// URI or Standard format
	if strings.HasPrefix(o.DbName, "postgresql://") || strings.Contains(o.DbName, "=") {
		return o.DbName, nil
	}

	var parts []string
	if o.Host != "" {
		parts = append(parts, fmt.Sprintf("host=%s", o.Host))
	}
	if o.Port != pgDefaultPort {
		parts = append(parts, fmt.Sprintf("port=%d", o.Port))
	}
	if o.UserName != "" {
		parts = append(parts, fmt.Sprintf("user=%s", o.UserName))
	}
	if o.DbName != "" {
		parts = append(parts, fmt.Sprintf("dbname=%s", o.DbName))
	}

	return strings.Join(parts, " "), nil
}

func (o *Options) GetParams() []string {
	// TODO: dbname may be connection string itself, you have to prioritize it
	var args []string

	// General options:
	if o.FileName != "" {
		args = append(args, "--file", o.FileName)
	}
	if o.Format != "" {
		args = append(args, "--format", o.Format)
	}
	if o.Jobs != -1 && !o.SchemaOnly {
		args = append(args, "--jobs", strconv.FormatInt(int64(o.Jobs), 10))
	}
	if o.Verbose {
		args = append(args, "--verbose")
	}
	if o.Compression != -1 {
		args = append(args, "--compress", strconv.FormatInt(int64(o.Compression), 10))
	}
	if o.LockWaitTimeout != -1 {
		args = append(args, "--lock-wait-timeout", strconv.FormatInt(int64(o.Compression), 10))
	}
	if o.NoSync {
		args = append(args, "--no-sync")
	}

	// Options controlling the output content
	if o.DataOnly {
		args = append(args, "--data-only")
	}
	if o.Blobs {
		args = append(args, "--blobs")
	}
	if o.NoBlobs {
		args = append(args, "--no-blobs")
	}
	if o.Clean {
		args = append(args, "--clean")
	}
	if o.Create {
		args = append(args, "--create")
	}
	if len(o.Extension) > 0 {
		for _, item := range o.Extension {
			args = append(args, "--extension", item)
		}
	}
	if o.Encoding != "" {
		args = append(args, "--encoding", o.Encoding)
	}
	if len(o.Schema) > 0 {
		for _, item := range o.Schema {
			args = append(args, "--schema", item)
		}
	}
	if len(o.ExcludeSchema) > 0 {
		for _, item := range o.ExcludeSchema {
			args = append(args, "--exclude-schema", item)
		}
	}
	if o.NoOwner {
		args = append(args, "--no-owner")
	}
	if o.SchemaOnly {
		args = append(args, "--schema-only")
	}
	if o.SuperUser != "" {
		args = append(args, "--superuser", o.SuperUser)
	}
	if len(o.Table) > 0 {
		for _, item := range o.Table {
			args = append(args, "--table", item)
		}
	}
	if len(o.ExcludeTable) > 0 {
		for _, item := range o.ExcludeTable {
			args = append(args, "--exclude-table", item)
		}
	}
	if o.NoPrivileges {
		args = append(args, "--no-privileges")
	}
	if o.DisableDollarQuoting {
		args = append(args, "--disable-dollar-quoting")
	}
	if o.DisableTriggers {
		//args = append(args, "--disable-triggers")
		panic("FIXME: --disable-triggers is not implemented")
	}
	if o.EnableRowSecurity {
		// TODO: Seems that this options affects COPY
		log.Warn().Msgf("FIXME: Seems that this options affects COPY and is not implemented")
		args = append(args, "--enable-row-security")
	}
	if len(o.ExcludeTableData) > 0 {
		for _, item := range o.ExcludeTableData {
			args = append(args, "--exclude-table-data", item)
		}
	}
	if o.ExtraFloatDigits != "" {
		args = append(args, "--extra-float-digits", o.ExtraFloatDigits)
	}
	if o.IfExists {
		args = append(args, "--if-exists")
	}
	if len(o.IncludeForeignData) > 0 {
		for _, item := range o.IncludeForeignData {
			args = append(args, "--include-foreign-data", item)
		}
	}
	if o.LoadViaPartitionRoot {
		args = append(args, "--load-via-partition-root")
	}
	if o.NoComments {
		args = append(args, "--no-comments")
	}
	if o.NoPublications {
		args = append(args, "--no-publications")
	}
	if o.NoSecurityLabels {
		args = append(args, "--no-security-labels")
	}
	if o.NoSubscriptions {
		args = append(args, "--no-subscriptions")
	}
	if o.NoSynchronizedSnapshots {
		args = append(args, "--no-synchronized-snapshots")
	}
	if o.NoTableSpaces {
		args = append(args, "--no-tablespaces")
	}
	if o.NoToastCompression {
		args = append(args, "--no-toast-compression")
	}
	if o.NoUnloggedTableData {
		args = append(args, "--no-unlogged-table-data")
	}
	if o.QuoteAllIdentifiers {
		args = append(args, "--quote-all-identifiers")
	}
	if o.Section != "" {
		args = append(args, "--section", o.Section)
	}
	if o.SerializableDeferrable {
		args = append(args, "--serializable-deferrable")
	}
	if o.Snapshot != "" {
		args = append(args, "--snapshot", o.Snapshot)
	}
	if o.StrictNames {
		//args = append(args, "--strict-names")
		panic("FIXME: --strict-names is not implemented")
	}
	if o.UseSetSessionAuthorization {
		// TODO: Need to check does it correctly work for data section
		args = append(args, "--use-set-session-authorization")
	}

	// Connection options:
	if o.DbName != "" {
		args = append(args, "--dbname", o.DbName)
	}
	if o.Host != "" && o.Host != "/var/run/postgres" {
		args = append(args, "--host", o.Host)
	}
	if o.Port != pgDefaultPort {
		args = append(args, "--port", strconv.FormatInt(int64(o.Port), 10))
	}
	if o.UserName != "" {
		args = append(args, "--username", o.UserName)
	}
	if o.NoPassword {
		args = append(args, "--no-password")
	}
	if o.Password {
		args = append(args, "--password")
	}
	if o.Role != "" {
		args = append(args, "--role", o.Role)
	}

	return args
}
