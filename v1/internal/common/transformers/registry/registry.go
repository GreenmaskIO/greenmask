// Copyright 2023 Greenmask
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

package registry

import (
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/transformers"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

var (
	errTransformerAlreadyExists = fmt.Errorf("transformer already exists")
)

var DefaultTransformerRegistry = NewTransformerRegistry()

type TransformerRegistry struct {
	M map[string]*utils.TransformerDefinition
}

func NewTransformerRegistry() *TransformerRegistry {
	return &TransformerRegistry{
		M: make(map[string]*utils.TransformerDefinition),
	}
}

func (tm *TransformerRegistry) Register(definition *utils.TransformerDefinition) error {
	if _, ok := tm.M[definition.Properties.Name]; ok {
		return fmt.Errorf("register transformer '%s': %w",
			definition.Properties.Name, errTransformerAlreadyExists)
	}
	tm.M[definition.Properties.Name] = definition
	return nil
}

func (tm *TransformerRegistry) MustRegister(definition *utils.TransformerDefinition) {
	if err := tm.Register(definition); err != nil {
		panic(err.Error())
	}
}

func (tm *TransformerRegistry) Get(name string) (*utils.TransformerDefinition, bool) {
	t, ok := tm.M[name]
	return t, ok
}

func init() {
	DefaultTransformerRegistry.MustRegister(transformers.ReplaceTransformerDefinition)
	DefaultTransformerRegistry.MustRegister(transformers.DictTransformerDefinition)
}
