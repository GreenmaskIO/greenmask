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

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var HashTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"Hash",
		"Generate hash of the text value using Scrypt hash function under the hood",
	),

	NewHashTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"function",
		"hash function name. Possible values sha1, sha256, sha512, md5",
	).SetDefaultValue([]byte("sha1")).
		SetRawValueValidator(validateHashFunctionsParameter),

	toolkit.MustNewParameter(
		"max_length",
		"limit length of hash function result",
	).SetDefaultValue([]byte("0")).
		SetRawValueValidator(validateMaxLengthParameter),
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
}

func NewHashTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
) (utils.Transformer, toolkit.ValidationWarnings, error) {
	p := parameters["column"]
	var columnName string
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["function"]
	var hashFunctionName string
	if _, err := p.Scan(&hashFunctionName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"function\" parameter: %w", err)
	}

	p = parameters["max_length"]
	var maxLength int
	if _, err := p.Scan(&maxLength); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"max_length\" parameter: %w", err)
	}

	var h hash.Hash
	var hashFunctionLength int
	switch hashFunctionName {
	case "md5":
		h = md5.New()
		hashFunctionLength = 16
	case "sha1":
		h = sha1.New()
		hashFunctionLength = 20
	case "sha256":
		h = sha256.New()
		hashFunctionLength = 32
	case "sha512":
		h = sha512.New()
		hashFunctionLength = 64
	default:
		return nil, nil, fmt.Errorf("unknown hash function \"%s\"", hashFunctionName)
	}

	return &HashTransformer{
		columnName:          columnName,
		affectedColumns:     affectedColumns,
		columnIdx:           idx,
		maxLength:           maxLength,
		hashBuf:             make([]byte, 0, hashFunctionLength),
		resultBuf:           make([]byte, hex.EncodedLen(hashFunctionLength)),
		encodedOutputLength: hex.EncodedLen(hashFunctionLength),
		h:                   h,
	}, nil, nil
}

func (ht *HashTransformer) GetAffectedColumns() map[int]string {
	return ht.affectedColumns
}

func (ht *HashTransformer) Init(ctx context.Context) error {
	return nil
}

func (ht *HashTransformer) Done(ctx context.Context) error {
	return nil
}

func (ht *HashTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(ht.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if val.IsNull {
		return r, nil
	}

	defer ht.h.Reset()
	_, err = ht.h.Write(val.Data)
	if err != nil {
		return nil, fmt.Errorf("unable to write raw data into writer: %w", err)
	}
	ht.hashBuf = ht.hashBuf[:0]
	ht.hashBuf = ht.h.Sum(ht.hashBuf)

	hex.Encode(ht.resultBuf, ht.hashBuf)

	maxLength := ht.encodedOutputLength
	if ht.maxLength > 0 && ht.encodedOutputLength > ht.maxLength {
		maxLength = ht.maxLength
	}

	if err := r.SetRawColumnValueByIdx(ht.columnIdx, toolkit.NewRawValue(ht.resultBuf[:maxLength], false)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func validateHashFunctionsParameter(p *toolkit.Parameter, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	functionName := string(v)
	switch functionName {
	case "md5", "sha1", "sha256", "sha512":
		return nil, nil
	}
	return toolkit.ValidationWarnings{
		toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterValue", functionName).
			SetMsg(`unknown hash function name`)}, nil
}

func validateMaxLengthParameter(p *toolkit.Parameter, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	max_length, err := strconv.ParseInt(string(v), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("error parsing \"max_length\" as integer: %w", err)
	}
	if max_length >= 0 {
		return nil, nil
	}
	return toolkit.ValidationWarnings{
		toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterValue", string(v)).
			SetMsg(`max_length parameter cannot be less than zero`)}, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(HashTransformerDefinition)
}
