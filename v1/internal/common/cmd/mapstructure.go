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

package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/greenmaskio/greenmask/v1/internal/config"
)

var (
	errWrongTypeProvided   = errors.New("wrong type provided")
	errFlagIsNotRegistered = errors.New("flag is not registered")
)

var (
	configFlag = Flag{
		Name:         "config",
		Shorthand:    "",
		Usage:        "Path to config file. Can be JSON or YAML format.",
		BindToConfig: false,
		Type:         FlagTypeString,
		Default:      "",
		IsRequired:   true,
	}
)

func initConfig(cfgFile string, cfg *config.Config) error {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("error reading config file, %s", err)
		}
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	decoderCfg := func(cfg *mapstructure.DecoderConfig) {
		cfg.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			config.ParamsToByteSliceHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		)
		cfg.ErrorUnused = true
	}

	if err := viper.Unmarshal(cfg, decoderCfg); err != nil {
		return fmt.Errorf("unmarshall config, %s", err)
	}

	if cfgFile != "" {
		// This solves problem with map structure described -> https://github.com/spf13/viper/issues/373
		// that caused issue in Greenmask https://github.com/GreenmaskIO/greenmask/issues/76
		if err := config.ParseTransformerParamsManually(cfgFile, cfg); err != nil {
			return fmt.Errorf("parse transformation config: %w", err)
		}
	}
	return nil
}

type Command struct {
	*cobra.Command
	parent       *Command
	otherConfigs map[string]any
	flags        []Flag
	configPath   string
	config       *config.Config
	IsRoot       bool
}

func MustRootCommand(cobraCmd *cobra.Command, version string, flags ...Flag) *Command {
	cmd, err := NewRootCommand(cobraCmd, version, flags...)
	if err != nil {
		panic(err)
	}
	return cmd
}

func NewRootCommand(cobraCmd *cobra.Command, version string, flags ...Flag) (*Command, error) {
	rootCmd, err := NewCommand(cobraCmd, flags...)
	if err != nil {
		return nil, fmt.Errorf("create root command: %w", err)
	}
	rootCmd.PersistentFlags().StringVar(&rootCmd.configPath, "config", "", "config file ")
	rootCmd.config = config.NewConfig()
	rootCmd.Command.Version = version
	rootCmd.IsRoot = true
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		cmd.InitDefaultCompletionCmd()
		cmd.InitDefaultHelpCmd()
		cmd.InitDefaultVersionFlag()
		if err := initConfig(rootCmd.configPath, rootCmd.config); err != nil {
			return fmt.Errorf("init config: %w", err)
		}
		return nil
	}
	return rootCmd, nil
}

func MustCommand(cobraCmd *cobra.Command, flags ...Flag) *Command {
	res, err := NewCommand(cobraCmd, flags...)
	if err != nil {
		panic(err)
	}
	return res
}

func NewCommand(cobraCmd *cobra.Command, flags ...Flag) (*Command, error) {
	res := &Command{
		Command: cobraCmd,
	}
	if err := res.registerFlags(flags...); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Command) bindToConfig(prefix string, flagName string) error {
	flag := c.Flags().Lookup(flagName)
	if flag == nil {
		return fmt.Errorf("lookup flag \"%s\": %w", flagName, errFlagIsNotRegistered)
	}
	fullFlagPath := flagName
	if prefix != "" {
		fullFlagPath = fmt.Sprintf("%s.%s", prefix, flagName)
	}
	if err := viper.BindPFlag(fullFlagPath, flag); err != nil {
		return fmt.Errorf("bind flag \"%s\": %w", fullFlagPath, err)
	}
	return nil
}

func (c *Command) registerBool(flag Flag) error {
	vv, ok := flag.Default.(bool)
	if !ok {
		return fmt.Errorf("flag %T is not a bool: %w", flag, errWrongTypeProvided)
	}
	c.Flags().BoolP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerString(flag Flag) error {
	vv, ok := flag.Default.(string)
	if !ok {
		return fmt.Errorf("flag %T is not a string: %w", flag, errWrongTypeProvided)
	}
	c.Flags().StringP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerInt(flag Flag) error {
	vv, ok := flag.Default.(int)
	if !ok {
		return fmt.Errorf("flag %T is not an int: %w", flag, errWrongTypeProvided)
	}
	c.Flags().IntP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerInt32(flag Flag) error {
	vv, ok := flag.Default.(int32)
	if !ok {
		return fmt.Errorf("flag %T is not an int32: %w", flag, errWrongTypeProvided)
	}
	c.Flags().Int32P(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerInt64(flag Flag) error {
	vv, ok := flag.Default.(int64)
	if !ok {
		return fmt.Errorf("flag %T is not an int64: %w", flag, errWrongTypeProvided)
	}
	c.Flags().Int64P(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerStringSlice(flag Flag) error {
	vv, ok := flag.Default.([]string)
	if !ok {
		return fmt.Errorf("flag %T is not a []string: %w", flag, errWrongTypeProvided)
	}
	c.Flags().StringSliceP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerFlag(opt Flag) error {
	switch opt.Type {
	case FlagTypeString:
		return c.registerString(opt)
	case FlagTypeBool:
		return c.registerBool(opt)
	case FlagTypeStringSlice:
		return c.registerStringSlice(opt)
	case FlagTypeInt:
		return c.registerInt(opt)
	case FlagRegisterInt32:
		return c.registerInt32(opt)
	case FlagRegisterInt64:
		return c.registerInt64(opt)

	default:
		return fmt.Errorf("flag type %s: %w", opt.Name, errUnknownFlagType)
	}
}

func (c *Command) register(opt Flag) error {
	if err := opt.Validate(); err != nil {
		return fmt.Errorf("validate flag: %w", err)
	}
	if err := c.registerFlag(opt); err != nil {
		return fmt.Errorf("register flag: %w", err)
	}
	if !opt.BindToConfig {
		return nil
	}
	if err := c.bindToConfig(opt.ConfigPathPrefix, opt.Name); err != nil {
		return fmt.Errorf("bind flag: %w", err)
	}
	if err := c.markIsRequired(opt); err != nil {
		return fmt.Errorf("mark flag as required: %w", err)
	}
	return nil
}

func (c *Command) markIsRequired(flag Flag) error {
	if !flag.IsRequired {
		return nil
	}
	if err := c.MarkFlagRequired(flag.Name); err != nil {
		return err
	}
	return nil
}

func (c *Command) registerFlags(flags ...Flag) error {
	for _, opt := range flags {
		if err := c.register(opt); err != nil {
			return fmt.Errorf("register flag: %w", err)
		}
	}
	return nil
}

func (c *Command) AddCommand(cmds ...*Command) *Command {
	for _, cmd := range cmds {
		c.Command.AddCommand(cmd.Command)
		// Link root command with parent.
		cmd.parent = c
		cmd.Command.Parent()
	}
	return c
}

func (c *Command) Parent() *Command {
	return c.parent
}

func (c *Command) MustGetConfig() *config.Config {
	if c.IsRoot {
		if c.config != nil {
			return c.config
		}
		panic("config is not set for root command")
	}
	return c.parent.MustGetConfig()
}
