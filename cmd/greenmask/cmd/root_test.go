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

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// TestExplicitConfigFile tests that when a config file is explicitly provided,
// it takes precedence over the default config file
func TestExplicitConfigFile(t *testing.T) {
	// Save original state to restore after test
	origCfgFile := cfgFile
	origViper := viper.GetViper()

	defer func() {
		// Reset the config file path and viper instance
		cfgFile = origCfgFile
		viper.Reset()
		*viper.GetViper() = *origViper
	}()

	// Setup temporary directory for testing
	tempDir, err := os.MkdirTemp("", "greenmask-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Mock os.UserConfigDir by setting XDG_CONFIG_HOME (Linux) or equivalent
	origConfigHome := os.Getenv("XDG_CONFIG_HOME")
	os.Setenv("XDG_CONFIG_HOME", tempDir)
	defer os.Setenv("XDG_CONFIG_HOME", origConfigHome)

	// Create an explicit config file
	explicitConfigPath := filepath.Join(tempDir, "config.yml")
	explicitConfigContent := `
log:
  level: info
`
	err = os.WriteFile(explicitConfigPath, []byte(explicitConfigContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write explicit config: %v", err)
	}

	// Create default directory to store a distraction config
	configDir := filepath.Join(tempDir, "greenmask")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create default config dir: %v", err)
	}

	// Create a default config file that should NOT be used
	testDefaultContent := `
log:
  level: debug
`
	defaultConfigPath := filepath.Join(configDir, "config.yml")
	if err := os.WriteFile(defaultConfigPath, []byte(testDefaultContent), 0644); err != nil {
		t.Fatalf("Failed to write test default config: %v", err)
	}

	// Set config file path to simulate --config flag BEFORE running initConfig
	cfgFile = explicitConfigPath

	// Verify that the config file path is set correctly before running the test
	if cfgFile != explicitConfigPath {
		t.Fatalf("Config file path not set correctly before test: got %s, want %s",
			cfgFile, explicitConfigPath)
	}

	// Clear viper config to ensure we're starting fresh
	viper.Reset()

	// Run initConfig directly
	initConfig()

	// Check if the explicit config was loaded instead of default
	assert.Equal(t, "info", viper.GetString("log.level"),
		"Explicit config should be loaded instead of default")

	// Check that the config file path is still the explicit path
	assert.Equal(t, explicitConfigPath, cfgFile,
		"Config file path should remain as explicitly provided path")
}

// TestDefaultConfigFile tests the behavior when no config file is provided
// and checks if the default config file in the platform-specific config directory is used
func TestDefaultConfigFile(t *testing.T) {
	// Save original state to restore after test
	origCfgFile := cfgFile
	origViper := viper.GetViper()
	defer func() {
		// Reset the config file path and viper instance
		cfgFile = origCfgFile
		viper.Reset()
		*viper.GetViper() = *origViper
	}()

	// Setup temporary directory for testing
	tempDir, err := os.MkdirTemp("", "greenmask")
	if err != nil {
		t.Fatalf("Failed to create temp home dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Mock os.UserConfigDir by setting XDG_CONFIG_HOME (Linux)
	origConfigHome := os.Getenv("XDG_CONFIG_HOME")
	os.Setenv("XDG_CONFIG_HOME", tempDir)
	defer os.Setenv("XDG_CONFIG_HOME", origConfigHome)

	// Create default config directory structure
	configDir := filepath.Join(tempDir, "greenmask")
	err = os.MkdirAll(configDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create config dir: %v", err)
	}

	// Create a test config file
	testConfigContent := `
log:
  level: debug
`
	err = os.WriteFile(filepath.Join(configDir, "config.yml"), []byte(testConfigContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	// Reset config file path to simulate no --config flag
	cfgFile = ""

	// Create a test command and run initConfig directly
	viper.Reset()
	initConfig()

	// Check if the default config was loaded by checking a value from the config
	assert.Equal(t, "debug", viper.GetString("log.level"),
		"Default config should be loaded from the standard config directory")

	// Check that the config file path was set to the default location
	assert.Equal(t, filepath.Join(configDir, "config.yml"), cfgFile,
		"Config file path should be set to default location")
}

// TestNoConfigFile tests the behavior when no config file is provided
// and there's no default config file
func TestNoConfigFile(t *testing.T) {
	// Save original state to restore after test
	origCfgFile := cfgFile
	origViper := viper.GetViper()

	defer func() {
		// Reset the config file path and viper instance
		cfgFile = origCfgFile
		viper.Reset()
		*viper.GetViper() = *origViper
	}()

	// Reset config file path to simulate no --config flag
	cfgFile = ""

	// Clear viper config to ensure we're starting fresh
	viper.Reset()

	// Run initConfig directly
	initConfig()

	// Check that the config file path is still empty
	assert.Equal(t, "", cfgFile,
		"Config file path should remain empty when no config file exists")
}
