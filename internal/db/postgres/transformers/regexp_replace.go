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

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RegexpReplaceTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"RegexpReplace",
		"Replace string using regular expression",
	),

	NewRegexpReplaceTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("varchar", "text", "bpchar"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"regexp",
		"regular expression",
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
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

func NewRegexpReplaceTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.ParameterDefinition) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, regexpStr, replace string
	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["regexp"]
	if _, err := p.Scan(&regexpStr); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "regexp" param: %w`, err)
	}

	p = parameters["replace"]
	if _, err := p.Scan(&replace); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "replace" param: %w`, err)
	}
	re, err := regexp.Compile(regexpStr)

	if err != nil {
		return nil, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("ParameterName", "regexp").
				AddMeta("ParameterValue", regexpStr).
				AddMeta("Error", err.Error()).
				SetMsg("cannot compile regular expression"),
		}, nil
	}

	return &RegexpReplaceTransformer{
		columnName:      columnName,
		regexp:          re,
		replace:         []byte(replace),
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil

}

func (rrt *RegexpReplaceTransformer) GetAffectedColumns() map[int]string {
	return rrt.affectedColumns
}

func (rrt *RegexpReplaceTransformer) Init(ctx context.Context) error {
	return nil
}

func (rrt *RegexpReplaceTransformer) Done(ctx context.Context) error {
	return nil
}

func (rrt *RegexpReplaceTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	v, err := r.GetRawColumnValueByIdx(rrt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if v.IsNull {
		return r, nil
	}

	v.Data = rrt.regexp.ReplaceAll(v.Data, rrt.replace)
	if err := r.SetRawColumnValueByIdx(rrt.columnIdx, v); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RegexpReplaceTransformerDefinition)
}
