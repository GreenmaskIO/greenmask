package toc

import (
	"flag"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	// pgBinPath - path to postgresql binaries
	pgBinPath string
	// tempDir - path to tmp directory for manual dumping
	tempDir string
	// connCreds - credentials for connection to DB
	connCreds string
	// deleteArtifacts - delete artifacts on exiting, default false
	deleteArtifacts bool
	// greenmaskBinPath - path to greenmask binary
	greenmaskBinPath string
)

func init() {
	flag.StringVar(&tempDir, "tempDir", "", "path to temp dump directory")
	flag.StringVar(&pgBinPath, "pgBinPath", "", "path to postgresql binaries")
	flag.StringVar(&connCreds, "connCreds", "", "connection creds to the DB")
	flag.BoolVar(&deleteArtifacts, "deleteArtifacts", false, "connection creds to the DB")
	flag.StringVar(&greenmaskBinPath, "greenmaskBinPath", "", "path to greenmask binary")
}

func init() {
	if err := logger.SetLogLevel("debug", "text"); err != nil {
		panic(err)
	}
}
