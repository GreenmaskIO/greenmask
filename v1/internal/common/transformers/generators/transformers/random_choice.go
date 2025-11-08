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
	"encoding/binary"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type RandomChoiceTransformer struct {
	values     []*commonmodels.ColumnRawValue
	byteLength int
	generator  generators.Generator
}

func NewRandomChoiceTransformer(values []*commonmodels.ColumnRawValue) *RandomChoiceTransformer {
	return &RandomChoiceTransformer{
		values:     values,
		byteLength: 4,
	}
}

func (rc *RandomChoiceTransformer) Transform(original []byte) (*commonmodels.ColumnRawValue, error) {
	resBytes, err := rc.generator.Generate(original)
	if err != nil {
		return nil, err
	}
	idx := int(binary.LittleEndian.Uint32(resBytes)) % len(rc.values)
	return rc.values[idx], nil
}

func (rc *RandomChoiceTransformer) GetRequiredGeneratorByteLength() int {
	return rc.byteLength
}

func (rc *RandomChoiceTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < rc.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", rc.byteLength, g.Size())
	}
	rc.generator = g
	return nil
}
