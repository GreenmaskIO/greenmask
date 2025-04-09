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
	"bytes"
	"context"
	"fmt"

	"text/template"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const TemplateRecordTransformerName = "TemplateRecord"

var TemplateRecordTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TemplateRecordTransformerName,
		"Modify the record using gotemplate",
	),
	NewTemplateRecordTransformer,

	toolkit.MustNewParameterDefinition(
		"template",
		"gotemplate string",
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"validate",
		"validate template result via PostgreSQL driver decoding",
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("false")),

	toolkit.MustNewParameterDefinition(
		"columns",
		"columns that supposed to be affected by the template. The list of columns will be checked for constraint violation",
	).SetIsColumnContainer(true).
		SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("[]")),
)

type TemplateRecordTransformer struct {
	template        string
	affectedColumns map[int]string
	tmpl            *template.Template
	buf             *bytes.Buffer
	tctx            *toolkit.RecordContext
	columns         []string
}

func NewTemplateRecordTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var templateStr string
	var columns []string
	affectedColumns := make(map[int]string)
	p := parameters["template"]
	if err := p.Scan(&templateStr); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"template\" param: %w", err)
	}

	t := template.New("tmpl").Funcs(toolkit.FuncMap())
	tmpl, err := t.Parse(templateStr)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing template: %w", err)
	}

	p = parameters["columns"]
	if err := p.Scan(&columns); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"columns\" param: %w", err)
	}

	var warnings toolkit.ValidationWarnings
	for num, columnName := range columns {
		idx, column, ok := driver.GetColumnByName(columnName)
		if !ok {
			warnings = append(warnings, toolkit.NewValidationWarning().
				AddMeta("ElementNum", num).
				AddMeta("ColumnName", column.Name).
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("column not found"))
			continue
		}

		warns := utils.ValidateSchema(driver.Table, column, nil)
		warnings = append(warnings, warns...)

		affectedColumns[idx] = columnName
	}

	return &TemplateRecordTransformer{
		template:        templateStr,
		affectedColumns: affectedColumns,
		tmpl:            tmpl,
		buf:             bytes.NewBuffer(nil),
		tctx:            toolkit.NewRecordContext(),
		columns:         columns,
	}, warnings, nil
}

func (sut *TemplateRecordTransformer) GetAffectedColumns() map[int]string {
	return sut.affectedColumns
}

func (sut *TemplateRecordTransformer) Init(ctx context.Context) error {
	return nil
}

func (sut *TemplateRecordTransformer) Done(ctx context.Context) error {
	return nil
}

func (sut *TemplateRecordTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	sut.tctx.SetRecord(r)

	if err := sut.tmpl.Execute(sut.buf, sut.tctx); err != nil {
		return nil, fmt.Errorf("error executing template: %w", err)
	}

	sut.buf.Reset()
	sut.tctx.Clean()
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(TemplateRecordTransformerDefinition)
}
