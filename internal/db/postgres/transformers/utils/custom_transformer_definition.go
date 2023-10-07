package utils

import (
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type CustomTransformerDefinition struct {
	Name                     string               `mapstructure:"name" yaml:"name" json:"name"`
	Description              string               `mapstructure:"description" yaml:"description" json:"description"`
	Executable               string               `mapstructure:"executable" yaml:"executable" json:"executable"`
	Args                     []string             `mapstructure:"args" yaml:"args" json:"args"`
	Parameters               []*toolkit.Parameter `mapstructure:"parameters" yaml:"parameters" json:"parameters"`
	Validate                 bool                 `mapstructure:"validate" yaml:"validate" json:"validate"`
	AutoDiscover             bool                 `mapstructure:"auto_discover" yaml:"auto_discover" json:"auto_discover"`
	ValidationTimeout        time.Duration        `mapstructure:"validation_timeout" yaml:"validation_timeout" json:"validation_timeout"`
	AutoDiscoveryTimeout     time.Duration        `mapstructure:"auto_discovery_timeout" yaml:"auto_discovery_timeout" json:"auto_discovery_timeout"`
	RowTransformationTimeout time.Duration        `mapstructure:"row_transformation_timeout" yaml:"row_transformation_timeout" json:"row_transformation_timeout"`
	ExpectedExitCode         int                  `mapstructure:"expected_exit_code" yaml:"expected_exit_code" json:"expected_exit_code"`
}
