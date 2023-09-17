package toc

import (
	"errors"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
	"os"
	"os/exec"
	"path"
)

var config = &domains.Config{
	Common: domains.Common{
		PgBinPath:     "/usr/local/opt/postgresql@16/bin",
		TempDirectory: "/tmp",
	},
	Log: domains.LogConfig{
		Level:  "debug",
		Format: "text",
	},
	Storage: domains.StorageConfig{
		Directory: &directory.Config{
			Path: "/tmp",
		},
	},
	Dump: domains.Dump{
		PgDumpOptions: pgdump.Options{
			DbName: "host=localhost user=postgres password=example dbname=demo port=54316",
			Jobs:   10,
		},
		Transformation: []*domains.Table{
			{
				Schema: "bookings",
				Name:   "flights",
				Transformers: []*domains.TransformerConfig{
					{
						Name: "RandomDate",
						Params: map[string]domains.ParamsValue{
							"min":    domains.ParamsValue("2023-01-01 00:00:00.0+03"),
							"max":    domains.ParamsValue("2023-01-02 00:00:00.0+03"),
							"column": domains.ParamsValue("scheduled_departure"),
						},
					},
					{
						Name: "RandomDate",
						Params: map[string]domains.ParamsValue{
							"min":    domains.ParamsValue("2023-02-02 01:00:00.0+03"),
							"max":    domains.ParamsValue("2023-03-03 00:00:00.0+03"),
							"column": domains.ParamsValue("scheduled_arrival"),
						},
					},
				},
			},
		},
	},
}

type BackwardCompatibilitySuite struct {
	suite.Suite
	tmpDirname     string
	runtimeTmpDir  string
	storageDir     string
	configFilePath string
}

func (suite *BackwardCompatibilitySuite) SetupSuite() {
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(connCreds, "-connCreds non-empty flag required")
	suite.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")

	// Creating tmp dir
	var err error
	suite.tmpDirname, err = os.MkdirTemp(tempDir, "backward_compatibility_test_")
	suite.Require().NoError(err, "error creating temp directory")
	log.Debug().Str("tempDir", tempDir).Msg("created temp directory")

	// Creating directory for storage
	suite.storageDir = path.Join(suite.tmpDirname, "storage")
	err = os.Mkdir(suite.storageDir, 0700)
	suite.Require().NoError(err, "error creating storage dir")

	// Creating directory for tmp
	suite.runtimeTmpDir = path.Join(suite.tmpDirname, "tmp")
	err = os.Mkdir(suite.runtimeTmpDir, 0700)
	suite.Require().NoError(err, "error creating tmp dir")

	config.Common.TempDirectory = suite.tmpDirname
	config.Storage.Directory.Path = suite.storageDir
	config.Dump.PgDumpOptions.DbName = connCreds
	config.Common.PgBinPath = pgBinPath

	suite.configFilePath = path.Join(suite.tmpDirname, "config.yml")
	confFile, err := os.Create(suite.configFilePath)
	suite.Require().NoError(err, "error creating config.yml file")
	defer confFile.Close()
	err = yaml.NewEncoder(confFile).Encode(config)
	suite.Require().NoError(err, "error encoding config into yaml")
}

func (suite *BackwardCompatibilitySuite) TestGreenmaskCompatibility() {
	suite.Run("dumping data using greenmask", func() {
		cmd := exec.Command(path.Join(greenmaskBinPath, "greenmask"),
			"--config", suite.configFilePath, "dump",
		)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		err := cmd.Run()
		suite.Require().NoError(err, "error running greenmask")
	})

	suite.Run("dumping data using greenmask", func() {
		entry, err := os.ReadDir(suite.storageDir)
		suite.Require().NoError(err, "error reading storage directory")
		suite.Require().Len(entry, 1, "unexpected directories in storage")
		lastDump := entry[0]
		suite.Require().True(lastDump.IsDir(), "unable to find last dump dir")

		cmd := exec.Command(path.Join(pgBinPath, "pg_restore"),
			"-l", path.Join(suite.storageDir, lastDump.Name()),
		)
		out, err := cmd.Output()
		if len(out) > 0 {
			log.Info().Str("output", string(out)).Msg("pg_restore stout forwarding")
		}
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				log.Warn().Str("stderr", string(exitErr.Stderr)).Msg("pg_restore run stderr forwarding")
				suite.Assert().NotContains(string(exitErr.Stderr), "warning", "received stderr contains warnings")
				suite.Assert().NotContains(string(exitErr.Stderr), "error", "received stderr contains errors")
			}
			suite.Require().NoError(err, "error performing pg_restore")
		}
	})
}

func (suite *BackwardCompatibilitySuite) TearDownSuite() {
	if deleteArtifacts {
		log.Debug().Msg("deleting tmp dir")
		if err := os.RemoveAll(suite.tmpDirname); err != nil {
			log.Warn().Err(err).Msg("error deleting tmp dir")
		}
	} else {
		log.Debug().Msg("keeping artifacts")
	}
}
