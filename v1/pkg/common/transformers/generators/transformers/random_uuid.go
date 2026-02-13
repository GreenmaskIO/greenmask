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

	"github.com/google/uuid"
	"github.com/greenmaskio/greenmask/internal/generators"
)

const uuidTransformerRequiredLength = 16

type RandomUuidTransformer struct {
	byteLength int
	generator  generators.Generator
}

func NewRandomUuidTransformer() *RandomUuidTransformer {
	return &RandomUuidTransformer{
		byteLength: uuidTransformerRequiredLength,
	}
}

func (ut *RandomUuidTransformer) Transform(data []byte) (uuid.UUID, error) {
	resBytes, err := ut.generator.Generate(data)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return uuid.FromBytes(resBytes)
}

func (ut *RandomUuidTransformer) GetRequiredGeneratorByteLength() int {
	return ut.byteLength
}

func (ut *RandomUuidTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < ut.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ut.byteLength, g.Size())
	}
	ut.generator = g
	return nil
}
