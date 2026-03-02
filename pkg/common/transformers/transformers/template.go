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

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	template2 "github.com/greenmaskio/greenmask/pkg/common/transformers/template"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/utils"
)

const TransformerNameTemplate = "Template"

var TemplateTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameTemplate,
		"Modify the value using gotemplate",
	),

	NewTemplateTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(models.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"template",
		"gotemplate string",
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"validate",
		"validate template result via PostgreSQL driver decoding",
	).SetRequired(false).
		SetDefaultValue(models.ParamsValue("false")),
)

type TemplateTransformer struct {
	columnName      string
	template        string
	validate        bool
	columnIdx       int
	affectedColumns map[int]string
	tmpl            *template.Template
	buf             *bytes.Buffer
	tctx            *ColumnContext
}

func NewTemplateTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	templateStr, err := getParameterValueWithName[string](ctx, parameters, "template")
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	validate, err := getParameterValueWithName[bool](ctx, parameters, "validate")
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	t := template.New("tmpl").Funcs(template2.FuncMap())
	tmpl, err := t.Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}

	return &TemplateTransformer{
		columnName: columnName,
		template:   templateStr,
		columnIdx:  column.Idx,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		tmpl:     tmpl,
		buf:      bytes.NewBuffer(nil),
		tctx:     NewColumnContext(column.TypeName, columnName),
		validate: validate,
	}, nil
}

func (t *TemplateTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *TemplateTransformer) Init(context.Context) error {
	return nil
}

func (t *TemplateTransformer) Done(context.Context) error {
	return nil
}

func (t *TemplateTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	t.tctx.SetRecord(r)

	if err := t.tmpl.Execute(t.buf, t.tctx); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	data := t.buf.Bytes()
	var res *models.ColumnRawValue
	if len(data) == 2 && data[0] == '\\' && data[1] == 'N' {
		res = models.NewColumnRawValue(nil, true)
	} else {
		if t.validate {
			if _, err := r.TableDriver().DecodeValueByColumnIdx(t.columnIdx, data); err != nil {
				return fmt.Errorf("validate template output via driver: %w", err)
			}
		}
		res = models.NewColumnRawValue(data, false)
	}
	if err := r.SetRawColumnValueByIdx(t.columnIdx, res); err != nil {
		return fmt.Errorf("set raw value: %w", err)
	}

	t.buf.Reset()
	return nil
}

func (t *TemplateTransformer) Describe() string {
	return TransformerNameTemplate
}

type ColumnContext struct {
	columnType string
	columnName string
	*template2.RecordContextReadOnly
}

func NewColumnContext(columnType string, columnName string) *ColumnContext {
	return &ColumnContext{
		columnType:            columnType,
		columnName:            columnName,
		RecordContextReadOnly: template2.NewRecordContextReadOnly(),
	}
}

func (cc *ColumnContext) GetColumnType() string {
	return cc.columnType
}

func (cc *ColumnContext) GetValue() (any, error) {
	return cc.GetColumnValue(cc.columnName)
}

func (cc *ColumnContext) GetRawValue() (any, error) {
	return cc.GetRawColumnValue(cc.columnName)
}

func (cc *ColumnContext) EncodeValue(v any) (any, error) {
	return cc.EncodeValueByColumn(cc.columnName, v)
}

func (cc *ColumnContext) DecodeValue(v any) (any, error) {
	return cc.DecodeValueByColumn(cc.columnType, v)
}
