package transformers

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators"
)

const (
	AllowApplyForReferenced    utils.MetaKey = "AllowApplyForReferenced"
	RequireHashEngineParameter utils.MetaKey = "RequireHashEngineParameter"
)

func getGenerateEngine(ctx context.Context, engineName string, size int) (generators.Generator, error) {
	switch engineName {
	case RandomEngineParameterName:
		return getRandomBytesGen(size)
	case HashEngineParameterName:
		salt, err := getSaltFromCtx(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting salt from context: %w", err)
		}
		return generators.GetHashBytesGen(salt, size)
	}
	return nil, fmt.Errorf("unknown engine %s", engineName)
}

func getSaltFromCtx(ctx context.Context) (salt []byte, err error) {
	saltAny := ctx.Value("salt")
	if saltAny != nil {
		salt = saltAny.([]byte)
	}
	return salt, nil
}

func getRandomBytesGen(size int) (generators.Generator, error) {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error generating random bytes sequence: %w", err)
	}
	seed := int64(binary.LittleEndian.Uint64(buf))
	return generators.NewRandomBytes(seed, size), nil
}

//func composeGeneratorWithProjectedOutput(hashFunction string, salt []byte, outputLength int) (generators.Generator, error) {
//	switch hashFunction {
//	case Sha1HashFunction:
//		gen, err := generators.NewHash(salt, hashFunction)
//		if err != nil {
//			return nil, err
//		}
//		var hashSize int
//		switch outputLength {
//		case 16:
//			hashSize = generators.MurMurHash128Size
//		case 8:
//			hashSize = generators.MurMurHash64Size
//		case 4:
//			hashSize = generators.MurMurHash32Size
//		default:
//			return nil, fmt.Errorf("unexpeted outputLength %d", outputLength)
//		}
//		murmurGen := generators.NewMurmurHash(0, hashSize)
//		return generators.NewProjector(gen, murmurGen), nil
//	default:
//		return nil, fmt.Errorf("unknown hash function %s", hashFunction)
//	}
//}
