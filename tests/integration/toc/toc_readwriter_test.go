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

package toc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"

	toclib "github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

// Scenario
// 1.

type TocReadWriterSuite struct {
	suite.Suite
	tmpDir  string
	dumpDir string
}

func (suite *TocReadWriterSuite) SetupSuite() {
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(uri, "-uri non-empty flag required")

	var err error
	//suite.tmpDir = path.Join(tempDir, fmt.Sprintf("%d", time.Now().UnixMilli()))
	suite.tmpDir, err = os.MkdirTemp(tempDir, "toc_read_writer_test_")
	suite.Require().NoError(err, "error creating temp dir")
	suite.dumpDir = path.Join(suite.tmpDir, "pg_dump")
	log.Debug().Str("dir", suite.tmpDir).Msg("temp artifacts directory")

	// Prepare pg_dump for running
	cmd := exec.Command(
		path.Join(pgBinPath, "pg_dump"),
		"-Fd", "--dbname", uri, "-f", suite.dumpDir,
	)
	stdout, err := cmd.StdoutPipe()
	suite.Require().NoError(err, "unable to open stdout pipe")
	defer stdout.Close()
	stderr, err := cmd.StderrPipe()
	suite.Require().NoError(err, "unable to open stderr pipe")
	defer stderr.Close()

	// Run pg_dump and check pipe data as well as return code
	err = cmd.Start()
	suite.Require().NoError(err, "error starting pg_dump process")
	var stdoutOut []byte
	buf := make([]byte, 1024)
	for {
		n, err := stdout.Read(buf)
		stdoutOut = append(stdoutOut, buf[:n]...)
		if err != nil {
			suite.Require().Contains(err.Error(), "EOF", "error reading stdout pipe")
			break
		}
	}

	var stderrOut []byte
	for {
		n, err := stderr.Read(buf)
		stderrOut = append(stderrOut, buf[:n]...)
		if err != nil {
			suite.Require().Contains(err.Error(), "EOF", "error reading stdout pipe")
			break
		}
	}

	suite.Require().NoError(err, "error reading from stderr pipe")
	if len(stderrOut) > 0 {
		log.Warn().Str("stderr", string(stderrOut)).Msg("pg_dump: received non empty stderr")
	}

	if len(stdoutOut) > 0 {
		log.Warn().Str("stderr", string(stdoutOut)).Msg("pg_dump: stdout forwarding")
	}

	suite.Assert().NotContains(string(stderrOut), "warning", "received stderr contains warnings")
	suite.Assert().NotContains(string(stderrOut), "error", "received stderr contains errors")
	err = cmd.Wait()
	suite.Require().NoError(err, "error running pg_dump")
}

func (suite *TocReadWriterSuite) TestReadWriteTocDat() {

	suite.Run("reading and writing toc.dat", func() {
		src, err := os.Open(path.Join(suite.dumpDir, "toc.dat"))
		if err != nil {
			suite.Require().NoError(err, "error opening original toc.dat file")
		}
		defer src.Close()
		dest, err := os.Create(path.Join(suite.dumpDir, "new_toc.dat"))
		if err != nil {
			suite.Require().NoError(err, "error creating decoded new_toc.dat file")
		}
		defer dest.Close()

		reader := toclib.NewReader(src)
		toc, err := reader.Read()
		if err != nil {
			suite.Require().NoError(err, "error reading toc file using toc library")
		}

		writer := toclib.NewWriter(dest)
		if err := writer.Write(toc); err != nil {
			suite.Require().NoError(err, "error writing toc file using toc library")
		}
	})

	suite.Run("hexdiff", func() {
		originalTocFile, err := os.Open(path.Join(suite.dumpDir, "toc.dat"))
		if err != nil {
			suite.Require().NoError(err, "error opening original toc.dat file")
		}
		defer originalTocFile.Close()
		newTocFile, err := os.Open(path.Join(suite.dumpDir, "new_toc.dat"))
		if err != nil {
			suite.Require().NoError(err, "error opening decoded new_toc.dat file")
		}
		defer newTocFile.Close()

		originalTocBytes, err := io.ReadAll(originalTocFile)
		suite.Require().NoError(err, "error reading original toc file")

		newTocBytes, err := io.ReadAll(newTocFile)
		suite.Require().NoError(err, "error reading decoded new_toc.dat file")

		isEqual := suite.Assert().Equal(originalTocBytes, newTocBytes, "toc files are unequal")
		if !isEqual {
			log.Debug().Msg("performing pretty printed diff comparison")
			cmd := exec.Command(
				"bash", "-c",
				fmt.Sprintf("diff -y <(xxd %s/toc.dat) <(xxd %s/new_toc.dat) ",
					suite.dumpDir, suite.dumpDir,
				),
			)

			out, err := cmd.Output()
			if err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || (errors.As(err, &exitErr) && exitErr.ExitCode() != 0 && exitErr.ExitCode() != 1) {
					suite.Require().NoError(err, "error performing hex diff")
				}
			}
			log.Info().Msg("printing pretty hex diff")
			lineLength := slices.Index(out, '\n')
			originalName := "ORIGINAL_TOC"
			newName := "NEW_TOC"
			col1 := fmt.Sprintf("%[1]*s", -lineLength/2, fmt.Sprintf("%[1]*s", (lineLength+len(originalName))/4, originalName))
			col2 := fmt.Sprintf("%[1]*s", -lineLength/2, fmt.Sprintf("%[1]*s", (lineLength+len(newName))/4, newName))
			fmt.Println(strings.Repeat("-", lineLength))
			fmt.Printf("%s|%s\n", col1, col2)
			fmt.Println(strings.Repeat("-", lineLength))
			fmt.Println(string(out))
		}

	})

	suite.Run("try run pg_restore", func() {
		err := os.Rename(path.Join(suite.dumpDir, "toc.dat"), path.Join(suite.dumpDir, "original_toc.dat"))
		suite.Require().NoError(err, "error renaming toc dat file")
		func() {
			dst, err := os.Create(path.Join(suite.dumpDir, "toc.dat"))
			suite.Require().NoError(err, "error moving new_toc.dat")
			defer dst.Close()
			src, err := os.Open(path.Join(suite.dumpDir, "new_toc.dat"))
			suite.Require().NoError(err, "error moving new_toc.dat")
			defer src.Close()
			_, err = io.Copy(dst, src)
			suite.Require().NoError(err, "error moving new_toc.dat")
		}()
		cmd := exec.Command(
			path.Join(pgBinPath, "pg_restore"),
			"-v", "-l", suite.dumpDir,
		)

		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		log.Info().Msg("pg_restore stderr and stdout forwarding")
		err = cmd.Run()
		suite.Assert().NoError(err, "error running pg_restore")
	})

}

func (suite *TocReadWriterSuite) TearDownSuite() {
	if deleteArtifacts {
		log.Debug().Msg("deleting tmp dir")
		if err := os.RemoveAll(suite.tmpDir); err != nil {
			log.Warn().Err(err).Msg("error deleting tmp dir")
		}
	} else {
		log.Debug().Str("dir", suite.tmpDir).Msg("keeping artifacts")
	}
}
