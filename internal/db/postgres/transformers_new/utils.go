package transformers_new

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	Sha1HashFunction = "sha1"
)

var deterministicTransformerParameters = []*toolkit.ParameterDefinition{
	toolkit.MustNewParameterDefinition(
		"salt",
		"Secret salt for hash function hex encoded",
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"hash_function",
		"Hash function name",
	).SetRequired(true),
}

type newTransformerFunctionBase func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer, g generators.Generator) (utils.Transformer, toolkit.ValidationWarnings, error)

func deterministicTransformerProducer(newTransformer newTransformerFunctionBase, outputLength int) utils.NewTransformerFunc {

	return func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
		var saltHex, hashFunction string

		saltParam := parameters["max"]
		hashFunctionParam := parameters["hash_function"]

		if err := saltParam.Scan(&saltHex); err != nil {
			return nil, nil, fmt.Errorf(`unable to scan "salt" param: %w`, err)
		}
		salt := make([]byte, hex.EncodedLen(len(saltHex)))
		hex.Encode(salt, []byte(saltHex))

		if err := hashFunctionParam.Scan(&hashFunction); err != nil {
			return nil, nil, fmt.Errorf(`unable to scan "hash_function" param: %w`, err)
		}

		gen, err := composeGeneratorWithProjectedOutput(Sha1HashFunction, salt, outputLength)
		if err != nil {
			return nil, nil, err
		}
		return newTransformer(ctx, driver, parameters, gen)
	}

}

func randomTransformerProducer(newTransformer newTransformerFunctionBase) utils.NewTransformerFunc {
	return func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
		seed := time.Now().UnixNano()
		gen, err := generators.NewInt64Random(seed)
		if err != nil {
			return nil, nil, err
		}
		return newTransformer(ctx, driver, parameters, gen)
	}
}

func mergeParameters(commonParams, deterministicParams []*toolkit.ParameterDefinition) []*toolkit.ParameterDefinition {
	res := slices.Clone(commonParams)
	res = append(res, deterministicParams...)
	return res
}

func composeGeneratorWithProjectedOutput(hashFunction string, salt []byte, outputLength int) (generators.Generator, error) {
	switch hashFunction {
	case Sha1HashFunction:
		sha1Gen := generators.NewSha1(salt)
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
		return generators.NewProjector(sha1Gen, murmurGen), nil
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

		randomTransformerProducer(baseNewFunc),

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
