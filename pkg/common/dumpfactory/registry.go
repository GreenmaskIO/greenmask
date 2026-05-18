package dumpfactory

import (
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

var (
	ErrAlreadyRegistered = errors.New("factory already registered for this kind")
	ErrUnknownKind       = errors.New("unknown factory kind")
)

type Registry[Kind comparable, Spec any, Dumper any] struct {
	factories map[Kind]interfaces.DumpFactory[Kind, Spec, Dumper]
}

func NewRegistry[Kind comparable, Spec any, Dumper any]() *Registry[Kind, Spec, Dumper] {
	return &Registry[Kind, Spec, Dumper]{
		factories: make(map[Kind]interfaces.DumpFactory[Kind, Spec, Dumper]),
	}
}

func (r *Registry[Kind, Spec, Dumper]) Register(factory interfaces.DumpFactory[Kind, Spec, Dumper]) error {
	k := factory.Kind()
	if _, ok := r.factories[k]; ok {
		return fmt.Errorf("kind '%v': %w", k, ErrAlreadyRegistered)
	}
	r.factories[k] = factory
	return nil
}

func (r *Registry[Kind, Spec, Dumper]) Get(kind Kind) (interfaces.DumpFactory[Kind, Spec, Dumper], error) {
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

func NewObjectDumpFactoryRegistry() interfaces.ObjectDumpFactoryRegistry {
	return NewRegistry[commonmodels.ObjectKind, commonmodels.ObjectDumpSpec, interfaces.ObjectDumper]()
}

func NewSchemaDumpFactoryRegistry() interfaces.SchemaDumpFactoryRegistry {
	return NewRegistry[commonmodels.SchemaDumpKind, commonmodels.SchemaDumpSpec, interfaces.SchemaDumper]()
}
