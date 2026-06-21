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

package restorefactory

import (
	"errors"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var (
	ErrAlreadyRegistered = errors.New("factory already registered for this kind")
	ErrUnknownKind       = errors.New("unknown factory kind")
)

type Registry[Kind comparable, Spec any, R any] struct {
	factories map[Kind]core.RestoreFactory[Kind, Spec, R]
}

func NewRegistry[Kind comparable, Spec any, R any]() *Registry[Kind, Spec, R] {
	return &Registry[Kind, Spec, R]{
		factories: make(map[Kind]core.RestoreFactory[Kind, Spec, R]),
	}
}

func (r *Registry[Kind, Spec, R]) Register(factory core.RestoreFactory[Kind, Spec, R]) error {
	k := factory.Kind()
	if _, ok := r.factories[k]; ok {
		return fmt.Errorf("kind '%v': %w", k, ErrAlreadyRegistered)
	}
	r.factories[k] = factory
	return nil
}

func (r *Registry[Kind, Spec, R]) Get(kind Kind) (core.RestoreFactory[Kind, Spec, R], error) {
	factory, ok := r.factories[kind]
	if !ok {
		return nil, fmt.Errorf("kind '%v': %w", kind, ErrUnknownKind)
	}
	return factory, nil
}

func (r *Registry[Kind, Spec, R]) New(kind Kind, spec Spec) (R, error) {
	factory, err := r.Get(kind)
	if err != nil {
		var zero R
		return zero, err
	}
	return factory.New(spec)
}

func NewObjectRestoreFactoryRegistry() core.ObjectRestoreFactoryRegistry {
	return NewRegistry[core.ObjectKind, core.ObjectRestoreSpec, core.ObjectRestorer]()
}

func NewSchemaRestoreFactoryRegistry() core.SchemaRestoreFactoryRegistry {
	return NewRegistry[core.SchemaObjectKind, core.SchemaRestoreSpec, core.SchemaRestorer]()
}
