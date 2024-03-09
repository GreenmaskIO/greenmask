package transformers_new

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	Sha1HashFunction = "sha1"
)

type UnifiedTransformer interface {
	Init(ctx context.Context) error
	Done(ctx context.Context) error
	Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error)
	GetAffectedColumns() map[int]string
	SetGenerator(g generators.Generator) error
	GetRequiredGeneratorByteLength() int
}

var deterministicTransformerParameters = []*toolkit.ParameterDefinition{
	toolkit.MustNewParameterDefinition(
		"salt",
		"Secret salt for hash function hex encoded",
	),
}

type newTransformerFunctionBase func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (UnifiedTransformer, toolkit.ValidationWarnings, error)

func deterministicTransformerProducer(newTransformer newTransformerFunctionBase, outputLength int) utils.NewTransformerFunc {

	return func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
		t, warns, err := newTransformer(ctx, driver, parameters)
		if err != nil || warns.IsFatal() {
			return nil, warns, err
		}

		salt := getSaltFromCtx(ctx)

		hashFunctionName, err := getHashFunctionNameBySize(t.GetRequiredGeneratorByteLength())
		if err != nil {
			return nil, warns, fmt.Errorf("unable to determine hash function for deterministic transformer: %w", err)
		}

		var gen generators.Generator

		gen, err = generators.NewHash(salt, hashFunctionName)
		if err != nil {
			return nil, warns, fmt.Errorf("cannot create hash function backend: %w", err)
		}

		if err = t.SetGenerator(gen); err != nil {
			return nil, warns, fmt.Errorf("cannot set hash function generator to transformer: %w", err)
		}

		return t, warns, nil
	}

}

func getSaltFromCtx(ctx context.Context) []byte {
	saltAny := ctx.Value("salt")
	var salt []byte
	if saltAny != nil {
		saltHex := saltAny.([]byte)
		salt = make([]byte, hex.EncodedLen(len(saltHex)))
		hex.Encode(salt, saltHex)
	}
	return salt
}

func randomTransformerProducer(newTransformer newTransformerFunctionBase, outputLength int) utils.NewTransformerFunc {
	return func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
		t, warns, err := newTransformer(ctx, driver, parameters)
		if err != nil || warns.IsFatal() {
			return nil, warns, err
		}

		buf := make([]byte, 8)
		_, err = rand.Read(buf)
		if err != nil {
			return nil, warns, fmt.Errorf("error generating random bytes sequence: %w", err)
		}
		seed := int64(binary.LittleEndian.Uint64(buf))
		gen := generators.NewBytesRandom(seed, outputLength)
		if err = t.SetGenerator(gen); err != nil {
			return nil, warns, err
		}
		return t, warns, nil
	}
}

func mergeParameters(commonParams, deterministicParams []*toolkit.ParameterDefinition) []*toolkit.ParameterDefinition {
	res := slices.Clone(commonParams)
	res = append(res, deterministicParams...)
	return res
}

func getHashFunctionNameBySize(size int) (string, error) {
	if size <= 28 {
		return generators.Sha3224, nil
	} else if size <= 32 {
		return generators.Sha3256, nil
	} else if size <= 48 {
		return generators.Sha3384, nil
	} else if size <= 64 {
		return generators.Sha3512, nil
	}
	return "", fmt.Errorf("unable to find suitable hash function for requested %d size", size)
}

func composeGeneratorWithProjectedOutput(hashFunction string, salt []byte, outputLength int) (generators.Generator, error) {
	switch hashFunction {
	case Sha1HashFunction:
		gen, err := generators.NewHash(salt, hashFunction)
		if err != nil {
			return nil, err
		}
		var hashSize int
		switch outputLength {
		case 16:
			hashSize = generators.MurMurHash128Size
		case 8:
			hashSize = generators.MurMurHash64Size
		case 4:
			hashSize = generators.MurMurHash32Size
		default:
			return nil, fmt.Errorf("unexpeted outputLength %d", outputLength)
		}
		murmurGen := generators.NewMurmurHash(0, hashSize)
		return generators.NewProjector(gen, murmurGen), nil
	default:
		return nil, fmt.Errorf("unknown hash function %s", hashFunction)
	}
}

func registerRandomAndDeterministicTransformer(
	tr *utils.TransformerRegistry, transformerName, transformerDescription string,
	baseNewFunc newTransformerFunctionBase, params []*toolkit.ParameterDefinition,
	outputLength int,
) {
	random := utils.NewTransformerDefinition(
		utils.NewTransformerProperties(
			fmt.Sprintf("random.%s", transformerName),
			transformerDescription,
		),

		randomTransformerProducer(baseNewFunc, outputLength),

		params...,
	)
	tr.MustRegister(random)

	deterministic := utils.NewTransformerDefinition(
		utils.NewTransformerProperties(
			fmt.Sprintf("deterministic.%s", transformerName),
			transformerDescription,
		),

		deterministicTransformerProducer(baseNewFunc, outputLength),

		mergeParameters(params, deterministicTransformerParameters)...,
	)
	tr.MustRegister(deterministic)
}
