package core

type DumpFactory[Kind comparable, Spec any, Dumper any] interface {
	Kind() Kind
	New(spec Spec) (Dumper, error)
}

type ObjectDumpFactory = DumpFactory[ObjectKind, ObjectDumpSpec, ObjectDumper]
type SchemaDumpFactory = DumpFactory[SchemaObjectKind, SchemaDumpSpec, SchemaDumper]

// FactoryRegistry is a generic registry mapping kind keys to DumpFactory values.
// New looks up the factory by kind and delegates to its New method.
type FactoryRegistry[Kind comparable, Spec any, Dumper any] interface {
	Register(factory DumpFactory[Kind, Spec, Dumper]) error
	Get(kind Kind) (DumpFactory[Kind, Spec, Dumper], error)
	New(kind Kind, spec Spec) (Dumper, error)
}

type ObjectDumpFactoryRegistry = FactoryRegistry[ObjectKind, ObjectDumpSpec, ObjectDumper]
type SchemaDumpFactoryRegistry = FactoryRegistry[SchemaObjectKind, SchemaDumpSpec, SchemaDumper]
