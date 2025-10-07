// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transformers

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/sha3"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const HashTransformerName = "Hash"

var HashTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		HashTransformerName,
		"Generate hash of the text value using Scrypt hash function under the hood",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, false),

	NewHashTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"salt",
		"hex encoded salt string. This value may be provided via environment variable GREENMASK_GLOBAL_SALT",
	).SetGetFromGlobalEnvVariable("GREENMASK_GLOBAL_SALT"),

	commonparameters.MustNewParameterDefinition(
		"function",
		fmt.Sprintf("hash function name. Possible values: %s",
			strings.Join(
				[]string{sha1Name, sha256Name, sha512Name, sha3224Name, sha3256Name, sha384Name, sha5124Name, md5Name},
				", ",
			),
		),
	).SetDefaultValue([]byte(sha3256Name)).
		SetRawValueValidator(validateHashFunctionsParameter),

	commonparameters.MustNewParameterDefinition(
		"max_length",
		"limit length of hash function expected",
	).SetDefaultValue([]byte("0")).
		SetRawValueValidator(validateMaxLengthParameter),
)

const (
	sha1Name    = "sha1"
	sha256Name  = "sha256"
	sha512Name  = "sha512"
	sha3224Name = "sha3-224"
	sha3256Name = "sha3-254"
	sha384Name  = "sha3-384"
	sha5124Name = "sha3-512"
	md5Name     = "md5"
)

type HashTransformer struct {
	columnName          string
	affectedColumns     map[int]string
	columnIdx           int
	h                   hash.Hash
	maxLength           int
	encodedOutputLength int
	hashBuf             []byte
	resultBuf           []byte
	salt                []byte
}

func NewHashTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, err
	}

	hashFunctionName, err := getParameterValueWithName[string](ctx, parameters, "function")
	if err != nil {
		return nil, fmt.Errorf("unable to scan \"function\" parameter: %w", err)
	}

	maxLength, err := getParameterValueWithName[int](ctx, parameters, "max_length")
	if err != nil {
		return nil, err
	}

	var h hash.Hash
	var hashFunctionLength int
	switch hashFunctionName {
	case md5Name:
		h = md5.New()
		hashFunctionLength = 16
	case sha1Name:
		h = sha1.New()
		hashFunctionLength = 20
	case sha256Name:
		h = sha256.New()
		hashFunctionLength = 32
	case sha512Name:
		h = sha512.New()
		hashFunctionLength = 64
	case sha3224Name:
		h = sha3.New224()
		hashFunctionLength = 28
	case sha3256Name:
		h = sha3.New256()
		hashFunctionLength = 32
	case sha384Name:
		h = sha3.New384()
		hashFunctionLength = 48
	case sha5124Name:
		h = sha3.New512()
		hashFunctionLength = 64
	default:
		return nil, fmt.Errorf("unknown hash function \"%s\"", hashFunctionName)
	}

	salt, err := getParameterValueWithName[string](ctx, parameters, "salt")
	if err != nil {
		return nil, err
	}

	return &HashTransformer{
		columnName: columnName,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx:           column.Idx,
		maxLength:           maxLength,
		hashBuf:             make([]byte, 0, hashFunctionLength),
		resultBuf:           make([]byte, hex.EncodedLen(hashFunctionLength)),
		salt:                []byte(salt),
		encodedOutputLength: hex.EncodedLen(hashFunctionLength),
		h:                   h,
	}, nil
}

func (ht *HashTransformer) GetAffectedColumns() map[int]string {
	return ht.affectedColumns
}

func (ht *HashTransformer) Init(context.Context) error {
	return nil
}

func (ht *HashTransformer) Done(context.Context) error {
	return nil
}

func (ht *HashTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(ht.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if val.IsNull {
		return nil
	}

	defer ht.h.Reset()
	_, err = ht.h.Write(ht.salt)
	if err != nil {
		return fmt.Errorf("unable to write salt into writer: %w", err)
	}
	_, err = ht.h.Write(val.Data)
	if err != nil {
		return fmt.Errorf("unable to write raw data into writer: %w", err)
	}
	ht.hashBuf = ht.hashBuf[:0]
	ht.hashBuf = ht.h.Sum(ht.hashBuf)

	hex.Encode(ht.resultBuf, ht.hashBuf)

	maxLength := ht.encodedOutputLength
	if ht.maxLength > 0 && ht.encodedOutputLength > ht.maxLength {
		maxLength = ht.maxLength
	}

	if err := r.SetRawColumnValueByIdx(
		ht.columnIdx,
		commonmodels.NewColumnRawValue(ht.resultBuf[:maxLength], false),
	); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}

	return nil
}

func validateHashFunctionsParameter(
	ctx context.Context,
	_ *commonparameters.ParameterDefinition,
	v models.ParamsValue,
) error {
	functionName := string(v)
	switch functionName {
	case sha1Name, sha256Name, sha512Name, sha3224Name, sha3256Name, sha384Name, sha5124Name:
		return nil
	case md5Name:
		log.Ctx(ctx).
			Warn().
			Str("ParameterValue", functionName).
			Msg("md5 hash function is deprecated and will be removed in the future")
		validationcollector.FromContext(ctx).Add(
			commonmodels.NewValidationWarning().
				SetSeverity(commonmodels.ValidationSeverityWarning).
				AddMeta("ParameterValue", functionName).
				SetMsg(`md5 hash function is deprecated and will be removed in the future`))
		return nil
	}
	validationcollector.FromContext(ctx).Add(
		commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta("ParameterValue", functionName).
			SetMsg(`unknown hash function name`))
	return nil
}

func validateMaxLengthParameter(
	ctx context.Context,
	_ *commonparameters.ParameterDefinition,
	v models.ParamsValue,
) error {
	maxLength, err := strconv.ParseInt(string(v), 10, 32)
	if err != nil {
		validationcollector.FromContext(ctx).
			Add(commonmodels.NewValidationWarning().
				AddMeta("ParameterValue", string(v)).
				SetSeverity(commonmodels.ValidationSeverityError).
				SetError(err).
				SetMsg("error parsing integer"))
		return commonmodels.ErrFatalValidationError
	}
	if maxLength >= 0 {
		return nil
	}
	validationcollector.FromContext(ctx).
		Add(commonmodels.NewValidationWarning().
			AddMeta("ParameterValue", string(v)).
			SetSeverity(commonmodels.ValidationSeverityError).
			SetMsg(`max_length parameter cannot be less than zero`))
	return commonmodels.ErrFatalValidationError
}
