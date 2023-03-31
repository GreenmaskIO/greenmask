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
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"slices"

	"golang.org/x/crypto/scrypt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// TODO: Make length truncation

const (
	saltLength = 32
	bufLength  = 1024
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
		"salt",
		"salt for hash",
	),
)

type HashTransformer struct {
	salt            toolkit.ParamsValue
	columnName      string
	affectedColumns map[int]string
	columnIdx       int
	res             []byte
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

	var saltStr string
	var salt toolkit.ParamsValue
	p = parameters["salt"]
	if _, err := p.Scan(&saltStr); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	if saltStr == "" {
		b := make(toolkit.ParamsValue, saltLength)
		if _, err := crand.Read(b); err != nil {
			return nil, nil, err
		}
		salt = b
	} else {
		salt = toolkit.ParamsValue(saltStr)
	}

	return &HashTransformer{
		salt:            salt,
		columnName:      columnName,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
		res:             make([]byte, 0, bufLength),
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
	val, err := r.GetRawAttributeValueByIdx(ht.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if val.IsNull {
		return r, nil
	}

	dk, err := scrypt.Key(val.Data, ht.salt, 32768, 8, 1, 32)
	if err != nil {
		return nil, fmt.Errorf("cannot perform hash calculation: %w", err)
	}

	length := base64.StdEncoding.EncodedLen(len(dk))
	if len(ht.res) < length {
		slices.Grow(ht.res, length)
	}
	ht.res = ht.res[0:length]

	//base64.StdEncoding.EncodeToString(ht.res)
	base64.StdEncoding.Encode(ht.res, dk)
	if err := r.SetRawAttributeValueByIdx(ht.columnIdx, toolkit.NewRawValue(ht.res, false)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(HashTransformerDefinition)
}
