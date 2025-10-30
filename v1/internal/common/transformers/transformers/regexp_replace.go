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
	"regexp"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const RegexpReplaceTransformerName = "RegexpReplace"

var RegexpReplaceTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RegexpReplaceTransformerName,
		"Replace string using regular expression",
	),

	NewRegexpReplaceTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar", "char", "bpchar", "citext"),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"regexp",
		"regular expression",
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"replace",
		"replacement value",
	).SetRequired(true),
)

type RegexpReplaceTransformer struct {
	columnName      string
	columnIdx       int
	regexp          *regexp.Regexp
	replace         []byte
	affectedColumns map[int]string
}

func NewRegexpReplaceTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	regexpStr, err := getParameterValueWithName[string](ctx, parameters, "regexp")
	if err != nil {
		return nil, fmt.Errorf("get \"regexp\" param: %w", err)
	}

	replace, err := getParameterValueWithName[string](ctx, parameters, "replace")
	if err != nil {
		return nil, fmt.Errorf("get \"regexp\" param: %w", err)
	}

	re, err := regexp.Compile(regexpStr)
	if err != nil {
		validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta("ParameterName", "regexp").
			AddMeta("ParameterValue", regexpStr).
			AddMeta("Error", err.Error()).
			SetMsg("cannot compile regular expression"))
		return nil, fmt.Errorf("compile regexp: %w", err)
	}

	return &RegexpReplaceTransformer{
		columnName: columnName,
		regexp:     re,
		replace:    []byte(replace),
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx: column.Idx,
	}, nil
}

func (rrt *RegexpReplaceTransformer) GetAffectedColumns() map[int]string {
	return rrt.affectedColumns
}

func (rrt *RegexpReplaceTransformer) Init(context.Context) error {
	return nil
}

func (rrt *RegexpReplaceTransformer) Done(context.Context) error {
	return nil
}

func (rrt *RegexpReplaceTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	v, err := r.GetRawColumnValueByIdx(rrt.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if v.IsNull {
		return nil
	}

	v.Data = rrt.regexp.ReplaceAll(v.Data, rrt.replace)
	if err := r.SetRawColumnValueByIdx(rrt.columnIdx, v); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}
