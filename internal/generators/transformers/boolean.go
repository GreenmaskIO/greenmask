package transformers

import (
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
)

type Boolean struct {
	generator  generators.Generator
	byteLength int
}

func NewBoolean() *Boolean {
	return &Boolean{
		byteLength: 1,
	}
}

func (b *Boolean) GetRequiredGeneratorByteLength() int {
	return b.byteLength
}

func (b *Boolean) SetGenerator(g generators.Generator) error {
	if g.Size() < b.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", b.byteLength, g.Size())
	}
	b.generator = g
	return nil
}

func (b *Boolean) Transform(original []byte) (bool, error) {
	resBytes, err := b.generator.Generate(original)
	if err != nil {
		return false, err
	}
	return resBytes[0]%2 == 0, nil
}
