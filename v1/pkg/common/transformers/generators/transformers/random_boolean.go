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

package transformers

import (
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
)

type RandomBoolean struct {
	generator  generators.Generator
	byteLength int
}

func NewRandomBoolean() *RandomBoolean {
	return &RandomBoolean{
		byteLength: 1,
	}
}

func (b *RandomBoolean) GetRequiredGeneratorByteLength() int {
	return b.byteLength
}

func (b *RandomBoolean) SetGenerator(g generators.Generator) error {
	if g.Size() < b.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", b.byteLength, g.Size())
	}
	b.generator = g
	return nil
}

func (b *RandomBoolean) Transform(original []byte) (bool, error) {
	resBytes, err := b.generator.Generate(original)
	if err != nil {
		return false, err
	}
	return resBytes[0]%2 == 0, nil
}
