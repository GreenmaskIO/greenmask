package transformers

import (
	"github.com/greenmaskio/greenmask/pkg/generators"
)

type Transformer interface {
	GetRequiredGeneratorByteLength() int
	SetGenerator(g generators.Generator) error
}
