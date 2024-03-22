package transformers

import (
	"encoding/binary"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type ChoiceTransformer struct {
	values     []*toolkit.RawValue
	byteLength int
	generator  generators.Generator
}

func NewChoiceTransformer(values []*toolkit.RawValue) *ChoiceTransformer {
	return &ChoiceTransformer{
		values:     values,
		byteLength: 4,
	}
}

func (rc *ChoiceTransformer) Transform(original []byte) (*toolkit.RawValue, error) {
	resBytes, err := rc.generator.Generate(original)
	if err != nil {
		return nil, err
	}
	idx := int(binary.LittleEndian.Uint32(resBytes)) % len(rc.values)
	return rc.values[idx], nil
}

func (rc *ChoiceTransformer) GetRequiredGeneratorByteLength() int {
	return rc.byteLength
}

func (rc *ChoiceTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < rc.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", rc.byteLength, g.Size())
	}
	rc.generator = g
	return nil
}
