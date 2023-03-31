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

package utils

import (
	"fmt"
)

var DefaultTransformerRegistry = NewTransformerRegistry()

type TransformerRegistry struct {
	M map[string]*Definition
}

func NewTransformerRegistry() *TransformerRegistry {
	return &TransformerRegistry{
		M: make(map[string]*Definition),
	}
}

func (tm *TransformerRegistry) Register(definition *Definition) error {
	if _, ok := tm.M[definition.Properties.Name]; ok {
		return fmt.Errorf("unable to register transformer: transformer with Name %s already exists",
			definition.Properties.Name)
	}
	tm.M[definition.Properties.Name] = definition
	return nil
}

func (tm *TransformerRegistry) MustRegister(definition *Definition) {
	if err := tm.Register(definition); err != nil {
		panic(err.Error())
	}
}

func (tm *TransformerRegistry) Get(name string) (*Definition, bool) {
	t, ok := tm.M[name]
	return t, ok
}
