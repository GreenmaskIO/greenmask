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
