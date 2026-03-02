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
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	config2 "github.com/greenmaskio/greenmask/pkg/config"
)

var (
	errWrongTypeProvided   = errors.New("wrong type provided")
	errFlagIsNotRegistered = errors.New("flag is not registered")
)

func initConfig(cfgFile string, cfg *config2.Config) error {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	decoderCfg := func(cfg *mapstructure.DecoderConfig) {
		cfg.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			config2.ParamsToByteSliceHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		)
		cfg.ErrorUnused = true
	}
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("error reading config file, %s", err)
		}
		if err := viper.Unmarshal(cfg, decoderCfg); err != nil {
			return fmt.Errorf("unmarshall config, %s", err)
		}
		// This solves problem with map structure described -> https://github.com/spf13/viper/issues/373
		// that caused issue in Greenmask https://github.com/GreenmaskIO/greenmask/issues/76
		if err := config2.ParseTransformerParamsManually(cfgFile, cfg); err != nil {
			return fmt.Errorf("parse transformation config: %w", err)
		}
	}
	return nil
}

type Command struct {
	*cobra.Command
	parent     *Command
	configPath string
	config     *config2.Config
	IsRoot     bool
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
	rootCmd.PersistentFlags().StringVar(
		&rootCmd.configPath, "config", "",
		"Path to config file. Can be JSON or YAML format.",
	)
	rootCmd.config = config2.NewConfig()
	rootCmd.Version = version
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

func (c *Command) getFlagSet(f Flag) *pflag.FlagSet {
	if f.IsPersistent {
		return c.PersistentFlags()
	}
	return c.Flags()
}

func (c *Command) bindToConfig(p string, f Flag) error {
	flag := c.getFlagSet(f).Lookup(f.Name)
	if flag == nil {
		return fmt.Errorf("lookup flag \"%s\": %w", f.Name, errFlagIsNotRegistered)
	}
	fullFlagPath := f.Name
	if p != "" {
		fullFlagPath = fmt.Sprintf("%s.%s", p, f.Name)
	}
	if err := viper.BindPFlag(fullFlagPath, flag); err != nil {
		return fmt.Errorf("bind flag \"%s\": %w", fullFlagPath, err)
	}
	return nil
}

func (c *Command) markIsRequired(flag Flag) error {
	if !flag.IsRequired {
		return nil
	}
	if flag.IsPersistent {
		if err := c.MarkPersistentFlagRequired(flag.Name); err != nil {
			return err
		}
		return nil
	}
	if err := c.MarkFlagRequired(flag.Name); err != nil {
		return err
	}
	return nil
}

func (c *Command) registerBool(flag Flag) error {
	vv, ok := flag.Default.(bool)
	if !ok {
		return fmt.Errorf("flag %T is not a bool: %w", flag, errWrongTypeProvided)
	}
	if flag.Dest != nil {
		boolPtr, ok := flag.Dest.(*bool)
		if !ok {
			return fmt.Errorf("flag destination %T is not *bool: %w", flag.Dest, errWrongTypeProvided)
		}
		c.getFlagSet(flag).BoolVarP(boolPtr, flag.Name, flag.Shorthand, vv, flag.Usage)
		return nil
	}
	c.getFlagSet(flag).BoolP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerString(flag Flag) error {
	vv, ok := flag.Default.(string)
	if !ok {
		return fmt.Errorf("flag %T is not a string: %w", flag, errWrongTypeProvided)
	}
	if flag.Dest != nil {
		strPtr, ok := flag.Dest.(*string)
		if !ok {
			return fmt.Errorf("flag destination %T is not *string: %w", flag.Dest, errWrongTypeProvided)
		}
		c.getFlagSet(flag).StringVarP(strPtr, flag.Name, flag.Shorthand, vv, flag.Usage)
		return nil
	}
	c.getFlagSet(flag).StringP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerInt(flag Flag) error {
	vv, ok := flag.Default.(int)
	if !ok {
		return fmt.Errorf("flag %T is not an int: %w", flag, errWrongTypeProvided)
	}
	if flag.Dest != nil {
		intPtr, ok := flag.Dest.(*int)
		if !ok {
			return fmt.Errorf("flag destination %T is not *int: %w", flag.Dest, errWrongTypeProvided)
		}
		c.getFlagSet(flag).IntVarP(intPtr, flag.Name, flag.Shorthand, vv, flag.Usage)
		return nil
	}
	c.getFlagSet(flag).IntP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerInt32(flag Flag) error {
	vv, ok := flag.Default.(int32)
	if !ok {
		return fmt.Errorf("flag %T is not an int32: %w", flag, errWrongTypeProvided)
	}
	if flag.Dest != nil {
		int32Ptr, ok := flag.Dest.(*int32)
		if !ok {
			return fmt.Errorf("flag destination %T is not *int32: %w", flag.Dest, errWrongTypeProvided)
		}
		c.getFlagSet(flag).Int32VarP(int32Ptr, flag.Name, flag.Shorthand, vv, flag.Usage)
		return nil
	}
	c.getFlagSet(flag).Int32P(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerInt64(flag Flag) error {
	vv, ok := flag.Default.(int64)
	if !ok {
		return fmt.Errorf("flag %T is not an int64: %w", flag, errWrongTypeProvided)
	}
	if flag.Dest != nil {
		int64Ptr, ok := flag.Dest.(*int64)
		if !ok {
			return fmt.Errorf("flag destination %T is not *int64: %w", flag.Dest, errWrongTypeProvided)
		}
		c.getFlagSet(flag).Int64VarP(int64Ptr, flag.Name, flag.Shorthand, vv, flag.Usage)
		return nil
	}
	c.getFlagSet(flag).Int64P(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerStringSlice(flag Flag) error {
	vv, ok := flag.Default.([]string)
	if !ok {
		return fmt.Errorf("flag %T is not a []string: %w", flag, errWrongTypeProvided)
	}
	if flag.Dest != nil {
		strSlicePtr, ok := flag.Dest.(*[]string)
		if !ok {
			return fmt.Errorf("flag destination %T is not *[]string: %w", flag.Dest, errWrongTypeProvided)
		}
		c.getFlagSet(flag).StringSliceVarP(strSlicePtr, flag.Name, flag.Shorthand, vv, flag.Usage)
		return nil
	}
	c.getFlagSet(flag).StringSliceP(flag.Name, flag.Shorthand, vv, flag.Usage)
	return nil
}

func (c *Command) registerDuration(flag Flag) error {
	vv, ok := flag.Default.(time.Duration)
	if !ok {
		return fmt.Errorf("flag %T is not a time.Duration: %w", flag, errWrongTypeProvided)
	}
	if flag.Dest != nil {
		durationPtr, ok := flag.Dest.(*time.Duration)
		if !ok {
			return fmt.Errorf("flag destination %T is not *time.Duration: %w", flag.Dest, errWrongTypeProvided)
		}
		c.getFlagSet(flag).DurationVarP(durationPtr, flag.Name, flag.Shorthand, vv, flag.Usage)
		return nil
	}
	c.getFlagSet(flag).DurationP(flag.Name, flag.Shorthand, vv, flag.Usage)
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
	case FlagTypeDuration:
		return c.registerDuration(opt)

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
	if err := c.bindToConfig(opt.ConfigPathPrefix, opt); err != nil {
		return fmt.Errorf("bind flag: %w", err)
	}
	if err := c.markIsRequired(opt); err != nil {
		return fmt.Errorf("mark flag as required: %w", err)
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

func (c *Command) MustGetConfig() *config2.Config {
	if c.IsRoot {
		if c.config != nil {
			return c.config
		}
		panic("config is not set for root command")
	}
	return c.parent.MustGetConfig()
}
