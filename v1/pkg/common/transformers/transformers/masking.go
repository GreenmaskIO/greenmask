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
	"strings"

	"github.com/ggwhite/go-masker"
	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	models2 "github.com/greenmaskio/greenmask/v1/pkg/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/pkg/common/validationcollector"
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
	MDefault    string = "default"
)

const TransformerNameMasking = "Masking"

var MaskingTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameMasking,
		"Mask a value using one of masking type",
	),

	NewMaskingTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		parameters.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypeClasses(models2.TypeClassText),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"type",
		"logical type of attribute (default, password, name, addr, email, mobile, tel, id, credit_card, url)",
	).SetRawValueValidator(maskerTypeValidator).
		SetDefaultValue(models2.ParamsValue(MDefault)),
)

type maskingFunction func(val string) string

type MaskingTransformer struct {
	columnName      string
	columnIdx       int
	masker          *masker.Masker
	maskingFunction maskingFunction
	affectedColumns map[int]string
}

func NewMaskingTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {

	var columnName string
	var dataType string
	var mf maskingFunction

	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("error getting column parameter: %w", err)
	}

	dataType, err = getParameterValueWithName[string](ctx, parameters, "type")
	if err != nil {
		return nil, fmt.Errorf("unable to scan \"type\" parameter: %w", err)
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
	case MDefault:
		mf = defaultMasker
	default:
		return nil, fmt.Errorf("wrong type: %s", dataType)
	}

	return &MaskingTransformer{
		columnName:      columnName,
		masker:          m,
		maskingFunction: mf,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx: column.Idx,
	}, nil
}

func (t *MaskingTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *MaskingTransformer) Init(context.Context) error {
	return nil
}

func (t *MaskingTransformer) Done(context.Context) error {
	return nil
}

func (t *MaskingTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if val.IsNull {
		return nil
	}

	maskedValue := t.maskingFunction(string(val.Data))
	err = r.SetRawColumnValueByIdx(t.columnIdx, models2.NewColumnRawValue([]byte(maskedValue), false))
	if err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func defaultMasker(v string) string {
	return strings.Repeat("*", len(v))
}

func maskerTypeValidator(
	ctx context.Context,
	_ *parameters.ParameterDefinition,
	v models2.ParamsValue,
) error {
	switch string(v) {
	case MDefault, MPassword, MName, MAddress, MEmail, MMobile, MTelephone, MID, MCreditCard, MURL:
		return nil
	}
	validationcollector.FromContext(ctx).Add(
		models2.NewValidationWarning().
			SetSeverity(models2.ValidationSeverityWarning).
			AddMeta("ParameterValue", string(v)).
			AddMeta("AllowedValues", []string{
				MDefault, MPassword, MName, MAddress, MEmail, MMobile, MTelephone, MID, MCreditCard, MURL,
			}).SetMsg(`unknown masking type`))
	return fmt.Errorf("unknown masking type: %w", models2.ErrFatalValidationError)
}

func (t *MaskingTransformer) Describe() string {
	return TransformerNameMasking
}
