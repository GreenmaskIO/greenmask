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
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

// EnvInterpolationSuite tests that environment variable interpolation works
// correctly in config file values. Uses the list-transformers command because
// it does not require a database or storage backend.
type EnvInterpolationSuite struct {
	suite.Suite
	tmpDir string
}

func (suite *EnvInterpolationSuite) SetupSuite() {
	suite.Require().NotEmpty(greenmaskBinPath, "GREENMASK_BIN_PATH required")
	var err error
	suite.tmpDir, err = os.MkdirTemp("", "env_interpolation_test_*")
	suite.Require().NoError(err)
}

func (suite *EnvInterpolationSuite) TearDownSuite() {
	_ = os.RemoveAll(suite.tmpDir)
}

// TestDefaultValueUsedWhenVarUnset verifies ${VAR:-default} expands to the
// provided default when the variable is not set in the environment.
func (suite *EnvInterpolationSuite) TestDefaultValueUsedWhenVarUnset() {
	cfgPath := filepath.Join(suite.tmpDir, "cfg_default.yml")
	suite.Require().NoError(os.WriteFile(cfgPath, []byte(`common:
  pg_bin_path: "/usr/bin"
  tmp_dir: ${GM_TEST_TMP_DIR:-/tmp}
log:
  level: ${GM_TEST_LOG_LEVEL:-info}
  format: text
storage:
  directory:
    path: /tmp
`), 0o644))

	cmd := exec.Command(filepath.Join(greenmaskBinPath, "greenmask"), "--config", cfgPath, "list-transformers")
	out, err := cmd.CombinedOutput()
	suite.Require().NoError(err, "expected success with defaults; output:\n%s", string(out))
}

// TestEnvVarOverridesDefault verifies that a set environment variable takes
// priority over the default value in ${VAR:-default}.
func (suite *EnvInterpolationSuite) TestEnvVarOverridesDefault() {
	cfgPath := filepath.Join(suite.tmpDir, "cfg_override.yml")
	suite.Require().NoError(os.WriteFile(cfgPath, []byte(`common:
  pg_bin_path: "/usr/bin"
  tmp_dir: /tmp
log:
  level: ${GM_TEST_LOG_LEVEL:-info}
  format: text
storage:
  directory:
    path: /tmp
`), 0o644))

	cmd := exec.Command(filepath.Join(greenmaskBinPath, "greenmask"), "--config", cfgPath, "list-transformers")
	cmd.Env = append(os.Environ(), "GM_TEST_LOG_LEVEL=debug")
	out, err := cmd.CombinedOutput()
	suite.Require().NoError(err, "expected success when env var overrides default; output:\n%s", string(out))
}

// TestRequiredVarMissingCausesError verifies that ${VAR?message} causes a
// startup error with the provided message when VAR is not set.
func (suite *EnvInterpolationSuite) TestRequiredVarMissingCausesError() {
	cfgPath := filepath.Join(suite.tmpDir, "cfg_required.yml")
	suite.Require().NoError(os.WriteFile(cfgPath, []byte(`common:
  pg_bin_path: "/usr/bin"
  tmp_dir: /tmp
log:
  level: info
  format: text
storage:
  directory:
    path: ${GM_TEST_REQUIRED_PATH?storage path is required}
`), 0o644))

	cmd := exec.Command(filepath.Join(greenmaskBinPath, "greenmask"), "--config", cfgPath, "list-transformers")
	// Run with empty environment so GM_TEST_REQUIRED_PATH is definitely unset.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	out, err := cmd.CombinedOutput()
	suite.Require().Error(err, "expected failure when required env var is unset")
	suite.Contains(string(out), "storage path is required",
		"error output should contain the ?message text; got:\n%s", string(out))
}

func TestEnvInterpolation(t *testing.T) {
	suite.Run(t, new(EnvInterpolationSuite))
}
