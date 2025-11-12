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
	"fmt"
	"slices"
	"strings"

	"github.com/ggwhite/go-masker"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	MPassword   string = "password"
	MName       string = "name"
	MAddress    string = "addr"
	MEmail      string = "email"
	MMobile     string = "mobile"
	MTelephone  string = "tel"
	MID         string = "id"
	MCreditCard string = "credit_card"
	MURL        string = "url"
	MPostcode   string = "postcode"
	MDefault    string = "default"
)

const MaskingTransformerName = "Masking"

var MaskingTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		MaskingTransformerName,
		"Mask a value using one of masking type",
	),

	NewMaskingTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes(
				"text", "varchar", "char", "bpchar", "citext",
			),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"type",
		"logical type of attribute (default, password, name, addr, email, mobile, tel, id, credit_card, url, postcode)",
	).SetRawValueValidator(maskerTypeValidator).
		SetDefaultValue(toolkit.ParamsValue(MDefault)),
)

type maskingFunction func(val string) string

type MaskingTransformer struct {
	columnName      string
	columnIdx       int
	masker          *masker.Masker
	maskingFunction maskingFunction
	affectedColumns map[int]string
}

func NewMaskingTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName string
	var dataType string
	var mf maskingFunction

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["type"]
	if err := p.Scan(&dataType); err != nil {
		return nil, nil, fmt.Errorf("unable to scan type param: %w", err)
	}

	var m = &masker.Masker{}

	switch dataType {
	case MPassword:
		mf = m.Password
	case MName:
		mf = m.Name
	case MAddress:
		mf = m.Address
	case MEmail:
		mf = m.Email
	case MMobile:
		mf = m.Mobile
	case MID:
		mf = m.ID
	case MTelephone:
		mf = m.Telephone
	case MCreditCard:
		mf = m.CreditCard
	case MURL:
		mf = m.URL
	case MPostcode:
		mf = postcodeMasker
	case MDefault:
		mf = defaultMasker
	default:
		return nil, nil, fmt.Errorf("wrong type: %s", dataType)
	}

	return &MaskingTransformer{
		columnName:      columnName,
		masker:          m,
		maskingFunction: mf,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (mt *MaskingTransformer) GetAffectedColumns() map[int]string {
	return mt.affectedColumns
}

func (mt *MaskingTransformer) Init(ctx context.Context) error {
	return nil
}

func (mt *MaskingTransformer) Done(ctx context.Context) error {
	return nil
}

func (mt *MaskingTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(mt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if val.IsNull {
		return r, nil
	}

	maskedValue := mt.maskingFunction(string(val.Data))
	err = r.SetRawColumnValueByIdx(mt.columnIdx, toolkit.NewRawValue([]byte(maskedValue), false))
	if err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func defaultMasker(v string) string {
	return strings.Repeat("*", len(v))
}

func postcodeMasker(v string) string {
	if len(v) <= 2 {
		return v
	}
	return v[:2] + strings.Repeat("*", len(v)-2)
}

func maskerTypeValidator(p *toolkit.ParameterDefinition, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	typeName := string(v)

	types := []string{MDefault, MPassword, MName, MAddress, MEmail, MMobile, MTelephone, MID, MCreditCard, MURL, MPostcode}
	if !slices.Contains(types, typeName) {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsgf("unknown type %s: must be one of %s", typeName, strings.Join(types, ", ")),
		}, nil
	}
	return nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(MaskingTransformerDefinition)
}
