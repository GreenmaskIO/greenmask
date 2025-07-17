package cmd

import (
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/config"
)

var (
	errUnknownFlagType        = errors.New("unknown option type")
	errFlagNameIsEmpty        = errors.New("option name is empty")
	errFlagDescriptionIsEmpty = errors.New("option description is empty")
	errDefaultValueIsEmpty    = errors.New("default value is empty")
)

type FlagType int

const (
	FlagTypeString FlagType = iota
	FlagTypeStringSlice
	FlagTypeInt
	FlagRegisterInt32
	FlagRegisterInt64
	FlagTypeBool
)

func (o FlagType) Validate() error {
	switch o {
	case FlagTypeString, FlagTypeStringSlice, FlagTypeInt, FlagTypeBool:
		return nil
	default:
		return fmt.Errorf("type %d is not supported: %w", o, errUnknownFlagType)
	}
}

type Flag struct {
	Name             string
	Shorthand        string
	Usage            string
	ConfigPathPrefix string
	Default          any
	BindToConfig     bool
	Type             FlagType
	ConfigDest       *config.Config
	IsRequired       bool
}

func (o *Flag) Validate() error {
	if o.Name == "" {
		return errFlagNameIsEmpty
	}
	if o.Usage == "" {
		return errFlagDescriptionIsEmpty
	}
	if o.Default == nil {
		return errDefaultValueIsEmpty
	}
	if err := o.Type.Validate(); err != nil {
		return fmt.Errorf("validate option type: %w", err)
	}
	return nil
}
