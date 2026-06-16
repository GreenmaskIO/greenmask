package factory

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dumpfactory"
	tabledump "github.com/greenmaskio/greenmask/pkg/mysql/dump/factory/data/table"
)

// NewObjectDumpRegistry builds the MySQL object-dump factory registry. The
// factories are storage- and session-free; runtime resources are injected into
// the dumpers at execution time. Per-table writer options (compression, pgzip,
// hex-encoding of binary columns) are not configurable yet — the factory uses
// its defaults.
func NewObjectDumpRegistry() (core.ObjectDumpFactoryRegistry, error) {
	reg := dumpfactory.NewObjectDumpFactoryRegistry()
	if err := reg.Register(tabledump.NewFactory()); err != nil {
		return nil, fmt.Errorf("register mysql table dump factory: %w", err)
	}
	return reg, nil
}

// NewSchemaDumpRegistry builds the MySQL schema-dump factory registry. Schema
// (DDL) dumping is not implemented yet, so the registry is currently empty.
func NewSchemaDumpRegistry() core.SchemaDumpFactoryRegistry {
	return dumpfactory.NewSchemaDumpFactoryRegistry()
}
