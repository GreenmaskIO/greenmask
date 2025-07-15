package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	errWrongTypeProvided   = errors.New("wrong type provided")
	errFlagIsNotRegistered = errors.New("flag is not registered")
)

type Command struct {
	*cobra.Command
	parent       *Command
	mainCfg      any
	otherConfigs map[string]any
	flags        []Flag
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
		return c.registerBool(opt)
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
	if err := c.registerString(opt); err != nil {
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
