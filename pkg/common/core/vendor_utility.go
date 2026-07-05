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

package core

import (
	"context"
	"io"
)

// VendorUtility identifies a vendor CLI tool that produced or consumed a dump
// (e.g. mysqldump, mysql, pg_dump). It is persisted into the dump metadata so
// restore/inspection can see which tool wrote the schema.
type VendorUtility struct {
	// Name is the executable name, e.g. "mysqldump".
	Name string `json:"name" yaml:"name"`
	// VersionString is the raw version token parsed out of the utility's
	// version output, e.g. "8.0.35" or "10.11.5-MariaDB".
	VersionString string `json:"version_string" yaml:"version_string"`
	// VersionParts is VersionString split into its dot-separated components,
	// e.g. ["8", "0", "35"]. Empty when VersionString is empty.
	VersionParts []string `json:"version_parts,omitempty" yaml:"version_parts,omitempty"`
}

// VendorUtilityProvider wraps a vendor CLI (mysqldump, mysql, pg_dump…). It owns
// the executable name, knows how to probe and parse its version, and executes
// it. It is injected into the engine-specific SchemaDumper/SchemaRestorer so
// they stop building and executing commands directly.
type VendorUtilityProvider interface {
	// Name returns the executable name (e.g. "mysqldump").
	Name() string
	// Version probes "<exe> --version", parses the output, and returns
	// {Name, Version}. Implementations cache the result.
	Version(ctx context.Context) (VendorUtility, error)
	// Stream runs the utility with args/env; stdin may be nil; when w is nil
	// stdout is forwarded to the log, otherwise streamed to w. This covers both
	// schema dump (stdout->storage) and schema restore (stdin<-file).
	Stream(ctx context.Context, args, env []string, stdin io.Reader, w io.Writer) error
}
