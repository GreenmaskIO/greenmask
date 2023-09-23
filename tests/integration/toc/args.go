package toc

import (
	"flag"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
	"os"
	"strconv"
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
	if err := logger.SetLogLevel("debug", "text"); err != nil {
		panic(err)
	}
}
