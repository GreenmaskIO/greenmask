// Package cmdrun provides a command initialization on the config settings.
//
// It injects all the dependencies in accordance to the config. For example
// if mysql is provided then the dumper of mysql must be initialized. It might initialize
// some common object and engine or purpose-specific.
package cmdrun
