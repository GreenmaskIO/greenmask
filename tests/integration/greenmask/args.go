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

package greenmask

import (
	"flag"
	"os"
	"strconv"

	"github.com/rs/zerolog"

	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	// pgBinPath - path to postgresql binaries
	pgBinPath string
	// tempDir - path to tmp directory for manual dumping
	tempDir string
	// uri - credentials for connection to DB
	uri string
	// deleteArtifacts - delete artifacts on exiting, default false
	deleteArtifacts bool
	// greenmaskBinPath - path to greenmask binary
	greenmaskBinPath string
)

const (
	pgBinPathEnvVarName     = "PG_BIN_PATH"
	tempDirEnvVarName       = "TEMP_DIR"
	uriEnvVarName           = "URI"
	greenmaskBinPathVarName = "GREENMASK_BIN_PATH"
	deleteArtifactsVarName  = "DELETE_ARTIFACTS"
)

func init() {
	flag.StringVar(&tempDir, "tempDir", "", "path to temp dump directory")
	flag.StringVar(&pgBinPath, "pgBinPath", "", "path to postgresql binaries")
	flag.StringVar(&uri, "uri", "", "connection creds to the DB")
	flag.StringVar(&greenmaskBinPath, "greenmaskBinPath", "", "path to greenmask binary")
	flag.BoolVar(&deleteArtifacts, "deleteArtifacts", false, "connection creds to the DB")

	if v := os.Getenv(tempDirEnvVarName); v != "" {
		tempDir = v
	}
	if v := os.Getenv(pgBinPathEnvVarName); v != "" {
		pgBinPath = v
	}
	if v := os.Getenv(uriEnvVarName); v != "" {
		uri = v
	}
	if v := os.Getenv(greenmaskBinPathVarName); v != "" {
		greenmaskBinPath = v
	}
	if v := os.Getenv(deleteArtifactsVarName); v != "" {
		vb, err := strconv.ParseBool(v)
		if err != nil {
			panic("error parsing bool in DELETE_ARTIFACTS")
		}
		deleteArtifacts = vb
	}

}

func init() {
	if err := logger.SetLogLevel(zerolog.LevelDebugValue, logger.LogFormatTextValue); err != nil {
		panic(err)
	}
}
