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

package pgrestore

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/utils/cmd_runner"
)

const pgRestoreExecutable = "pg_restore"

const pgDefaultPort = 5432

type PgRestore struct {
	BinPath string
}

func NewPgRestore(binPath string) *PgRestore {
	return &PgRestore{
		BinPath: binPath,
	}
}

func (pr *PgRestore) Run(ctx context.Context, options *Options) error {
	log.Debug().Msgf("pg_restore: %s %s\n", path.Join(pr.BinPath, pgRestoreExecutable), strings.Join(options.GetParams(), " "))
	return cmd_runner.Run(ctx, &log.Logger, path.Join(pr.BinPath, pgRestoreExecutable), options.GetParams()...)
}

// DataSectionSettings - settings for data section that changes behavior of dumpers
type DataSectionSettings struct {
	ExitOnError                      bool
	UsePgzip                         bool
	BatchSize                        int64
	OnConflictDoNothing              bool
	OverridingSystemValue            bool
	DisableTriggers                  bool
	SuperUser                        string
	UseSessionReplicationRoleReplica bool
}

type Options struct {
	// Custom
	DirPath string

	// General options:
	DbName   string `mapstructure:"dbname"`
	FileName string `mapstructure:"file"` // --file=FILENAME
	Format   string // Supports only directory format
	List     bool   `mapstructure:"list"`
	Verbose  bool   `mapstructure:"verbose"`
	Version  bool   `mapstructure:"version"`

	// Options controlling the output content
	DataOnly                   bool     `mapstructure:"data-only"`
	Clean                      bool     `mapstructure:"clean"`
	Create                     bool     `mapstructure:"create"`
	ExitOnError                bool     `mapstructure:"exit-on-error"`
	Index                      []string `mapstructure:"index"`
	Jobs                       int      `mapstructure:"jobs"`
	UseList                    string   `mapstructure:"use-list"`
	ListFormat                 string   `mapstructure:"list-format"`
	Schema                     []string `mapstructure:"schema"`
	ExcludeSchema              []string `mapstructure:"exclude-schema"`
	NoOwner                    bool     `mapstructure:"no-owner"`
	Function                   []string `mapstructure:"function"`
	SchemaOnly                 bool     `mapstructure:"schema-only"`
	SuperUser                  string   `mapstructure:"superuser"`
	Table                      []string `mapstructure:"table"`
	Trigger                    []string `mapstructure:"trigger"`
	NoPrivileges               bool     `mapstructure:"no-privileges"`
	SingleTransaction          bool     `mapstructure:"single-transaction"`
	DisableTriggers            bool     `mapstructure:"disable-triggers"`
	EnableRowSecurity          bool     `mapstructure:"enable-row-security"`
	IfExists                   bool     `mapstructure:"if-exists"`
	NoComments                 bool     `mapstructure:"no-comments"`
	NoDataForFailedTables      bool     `mapstructure:"no-data-for-failed-tables"`
	NoPublications             bool     `mapstructure:"no-publications"`
	NoSecurityLabels           bool     `mapstructure:"no-security-labels"`
	NoSubscriptions            bool     `mapstructure:"no-subscriptions"`
	NoTableAccessMethod        bool     `mapstructure:"no-table-access-method"`
	NoTableSpaces              bool     `mapstructure:"no-tablespaces"`
	Section                    string   `mapstructure:"section"`
	StrictNames                bool     `mapstructure:"strict-names"`
	UseSetSessionAuthorization bool     `mapstructure:"use-set-session-authorization"`
	// NoBlobs - is not supported by pg_restore itself but can be used in restore
	NoBlobs bool `mapstructure:"no-blobs"`

	// Custom options (not from pg_restore)
	// OnConflictDoNothing and Inserts were moved from pg_dump because we can generate insert
	// statements on fly if needed
	OnConflictDoNothing bool `mapstructure:"on-conflict-do-nothing"`
	Inserts             bool `mapstructure:"inserts"`
	RestoreInOrder      bool `mapstructure:"restore-in-order"`
	// OverridingSystemValue is a custom option that allows to use OVERRIDING SYSTEM VALUE for INSERTs
	OverridingSystemValue bool `mapstructure:"overriding-system-value"`
	// Use pgzip decompression instead of gzip
	Pgzip                            bool  `mapstructure:"pgzip"`
	BatchSize                        int64 `mapstructure:"batch-size"`
	UseSessionReplicationRoleReplica bool  `mapstructure:"use-session-replication-role-replica"`

	// Connection options:
	Host       string `mapstructure:"host"`
	Port       int    `mapstructure:"port"`
	UserName   string `mapstructure:"username"`
	NoPassword bool   `mapstructure:"no-password"`
	Password   bool   `mapstructure:"password"`
	Role       string `mapstructure:"role"`
}

func (o *Options) ToDataSectionSettings() *DataSectionSettings {
	return &DataSectionSettings{
		ExitOnError:                      o.ExitOnError,
		UsePgzip:                         o.Pgzip,
		BatchSize:                        o.BatchSize,
		OnConflictDoNothing:              o.OnConflictDoNothing,
		OverridingSystemValue:            o.OverridingSystemValue,
		DisableTriggers:                  o.DisableTriggers,
		SuperUser:                        o.SuperUser,
		UseSessionReplicationRoleReplica: o.UseSessionReplicationRoleReplica,
	}
}

func (o *Options) GetPgDSN() (string, error) {
	if strings.HasPrefix(o.DbName, "postgresql://") || strings.HasPrefix(o.DbName, "postgres://") || strings.Contains(o.DbName, "=") {
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
	if o.DbName != "" {
		args = append(args, "--dbname", o.DbName)
	}
	if o.FileName != "" {
		args = append(args, "--file", o.FileName)
	}
	if o.Format != "" {
		args = append(args, "--format", o.Format)
	}
	if o.List {
		args = append(args, "--list")
	}
	if o.Verbose {
		args = append(args, "--verbose")
	}
	if o.Version {
		args = append(args, "--version")
	}

	// Options controlling the output content
	if o.DataOnly {
		args = append(args, "--data-only")
	}

	if o.Clean {
		args = append(args, "--clean")
	}
	if o.Create {
		args = append(args, "--create")
	}
	if o.ExitOnError {
		args = append(args, "--exit-on-error")
	}
	if len(o.Index) > 0 {
		for _, item := range o.Index {
			args = append(args, "--index", item)
		}
	}
	if o.Jobs != -1 && !o.SchemaOnly {
		args = append(args, "--jobs", strconv.FormatInt(int64(o.Jobs), 10))
	}
	if o.UseList != "" {
		args = append(args, "--use-list", o.UseList)
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
	if len(o.Function) > 0 {
		for _, item := range o.Function {
			args = append(args, "--function", item)
		}
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
	if len(o.Trigger) > 0 {
		for _, item := range o.Trigger {
			args = append(args, "--trigger", item)
		}
	}
	if o.NoPrivileges {
		args = append(args, "--no-privileges")
	}
	if o.SingleTransaction {
		args = append(args, "--single-transaction")
	}
	if o.DisableTriggers {
		args = append(args, "--disable-triggers")
	}
	if o.EnableRowSecurity {
		// TODO: Seems that this options affects COPY
		log.Debug().Msgf("FIXME: Seems that this options affects COPY")
		args = append(args, "--enable-row-security")
	}
	if o.IfExists {
		args = append(args, "--if-exists")
	}
	if o.NoComments {
		args = append(args, "--no-comments")
	}
	if o.NoDataForFailedTables {
		args = append(args, "--no-data-for-failed-tables")
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
	if o.NoTableAccessMethod {
		args = append(args, "--no-table-access-method")
	}
	if o.NoTableSpaces {
		args = append(args, "--no-tablespaces")
	}
	if o.Section != "" {
		args = append(args, "--section", o.Section)
	}
	if o.StrictNames {
		args = append(args, "--strict-names")
	}
	if o.UseSetSessionAuthorization {
		args = append(args, "--use-set-session-authorization")
	}

	// Connection options:
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

	args = append(args, o.DirPath)

	return args
}
