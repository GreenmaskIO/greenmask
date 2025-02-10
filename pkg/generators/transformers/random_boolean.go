package transformers

import (
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/generators"
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
