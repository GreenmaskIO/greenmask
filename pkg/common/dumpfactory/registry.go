package dumpfactory

import (
	"errors"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var (
	ErrAlreadyRegistered = errors.New("factory already registered for this kind")
	ErrUnknownKind       = errors.New("unknown factory kind")
)

type Registry[Kind comparable, Spec any, Dumper any] struct {
	factories map[Kind]core.DumpFactory[Kind, Spec, Dumper]
}

func NewRegistry[Kind comparable, Spec any, Dumper any]() *Registry[Kind, Spec, Dumper] {
	return &Registry[Kind, Spec, Dumper]{
		factories: make(map[Kind]core.DumpFactory[Kind, Spec, Dumper]),
	}
}

func (r *Registry[Kind, Spec, Dumper]) Register(factory core.DumpFactory[Kind, Spec, Dumper]) error {
	k := factory.Kind()
	if _, ok := r.factories[k]; ok {
		return fmt.Errorf("kind '%v': %w", k, ErrAlreadyRegistered)
	}
	r.factories[k] = factory
	return nil
}

func (r *Registry[Kind, Spec, Dumper]) Get(kind Kind) (core.DumpFactory[Kind, Spec, Dumper], error) {
	factory, ok := r.factories[kind]
	if !ok {
		return nil, fmt.Errorf("kind '%v': %w", kind, ErrUnknownKind)
	}
	return factory, nil
}

func (r *Registry[Kind, Spec, Dumper]) New(kind Kind, spec Spec) (Dumper, error) {
	factory, err := r.Get(kind)
	if err != nil {
		var zero Dumper
		return zero, err
	}
	return factory.New(spec)
}

func NewObjectDumpFactoryRegistry() core.ObjectDumpFactoryRegistry {
	return NewRegistry[core.ObjectKind, core.ObjectDumpSpec, core.ObjectDumper]()
}

func NewSchemaDumpFactoryRegistry() core.SchemaDumpFactoryRegistry {
	return NewRegistry[core.SchemaObjectKind, core.SchemaDumpSpec, core.SchemaDumper]()
}
