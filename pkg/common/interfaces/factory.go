package interfaces

import (
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

// FactoryRegistry is a generic registry mapping kind keys to DumpFactory values.
// New looks up the factory by kind and delegates to its New method.
type FactoryRegistry[Kind comparable, Spec any, Dumper any] interface {
	Register(factory DumpFactory[Kind, Spec, Dumper]) error
	Get(kind Kind) (DumpFactory[Kind, Spec, Dumper], error)
	New(kind Kind, spec Spec) (Dumper, error)
}

type ObjectDumpFactoryRegistry = FactoryRegistry[commonmodels.ObjectKind, commonmodels.ObjectDumpSpec, ObjectDumper]
type SchemaDumpFactoryRegistry = FactoryRegistry[commonmodels.SchemaDumpKind, commonmodels.SchemaDumpSpec, SchemaDumper]
