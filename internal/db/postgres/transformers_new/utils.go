package transformers_new

import (
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
)

const (
	Sha1HashFunction = "sha1"
)

func getGeneratorWithProjectedOutput(hashFunction string, outputLength int) (generators.Generator, error) {
	switch hashFunction {
	case Sha1HashFunction:
		sha1Gen := generators.NewSha1([]byte("1234567"))
		murmurGen := generators.NewMurmurHash(0, generators.MurMurHash64Size)
		return generators.NewProjector(sha1Gen, murmurGen), nil
	default:
		return nil, fmt.Errorf("unknown hash function %s", hashFunction)
	}
}
