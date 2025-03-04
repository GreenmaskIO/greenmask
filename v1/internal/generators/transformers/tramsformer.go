package transformers

import (
	"github.com/greenmaskio/greenmask/internal/generators"
)

type Transformer interface {
	GetRequiredGeneratorByteLength() int
	SetGenerator(g generators.Generator) error
}
