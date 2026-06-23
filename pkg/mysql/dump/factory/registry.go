package factory

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dumpfactory"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	tabledump "github.com/greenmaskio/greenmask/pkg/mysql/dump/factory/data/table"
	schemadump "github.com/greenmaskio/greenmask/pkg/mysql/dump/factory/schema"
	"github.com/greenmaskio/greenmask/pkg/mysql/provider"
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

// NewSchemaDumpRegistry builds the MySQL schema-dump factory registry. The
// schema dumper is mysqldump-backed; storage and connection attributes are
// injected into the dumper at execution time, so the factory is resource-free.
func NewSchemaDumpRegistry() (core.SchemaDumpFactoryRegistry, error) {
	reg := dumpfactory.NewSchemaDumpFactoryRegistry()
	mysqldumpProvider := provider.NewMysqldumpProvider(utils.NewDefaultCmdProducer())
	if err := reg.Register(schemadump.NewFactory(mysqldumpProvider)); err != nil {
		return nil, fmt.Errorf("register mysql schema dump factory: %w", err)
	}
	return reg, nil
}
