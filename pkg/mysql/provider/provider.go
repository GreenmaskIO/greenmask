// Copyright 2025 Greenmask
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

// Package provider constructs the MySQL vendor-utility providers (mysqldump for
// the dump side, mysql for the restore side) on top of the generic
// vendorutility.CmdProvider, wiring in the MySQL "--version" output parser.
package provider

import (
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/vendorutility"
	mysqlversion "github.com/greenmaskio/greenmask/pkg/mysql/version"
)

const (
	// MysqldumpExecutable is the schema-dump CLI.
	MysqldumpExecutable = "mysqldump"
	// MysqlClientExecutable is the schema-restore CLI.
	MysqlClientExecutable = "mysql"
)

// NewMysqldumpProvider builds the mysqldump vendor-utility provider used by the
// schema dumper.
func NewMysqldumpProvider(cmd utils.CmdProducer, opts ...vendorutility.Option) *vendorutility.CmdProvider {
	return vendorutility.New(cmd, MysqldumpExecutable, mysqlversion.ParseUtility, opts...)
}

// NewMysqlClientProvider builds the mysql client vendor-utility provider used by
// the schema restorer.
func NewMysqlClientProvider(cmd utils.CmdProducer, opts ...vendorutility.Option) *vendorutility.CmdProvider {
	return vendorutility.New(cmd, MysqlClientExecutable, mysqlversion.ParseUtility, opts...)
}
