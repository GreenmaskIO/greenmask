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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	gmtemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const TemplateTransformerName = "Template"

var TemplateTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TemplateTransformerName,
		"Modify the value using gotemplate",
	),

	NewTemplateTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"template",
		"gotemplate string",
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"validate",
		"validate template result via PostgreSQL driver decoding",
	).SetRequired(false).
		SetDefaultValue(commonmodels.ParamsValue("false")),
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
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
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

	t := template.New("tmpl").Funcs(gmtemplate.FuncMap())
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

func (sut *TemplateTransformer) GetAffectedColumns() map[int]string {
	return sut.affectedColumns
}

func (sut *TemplateTransformer) Init(context.Context) error {
	return nil
}

func (sut *TemplateTransformer) Done(context.Context) error {
	return nil
}

func (sut *TemplateTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	sut.tctx.SetRecord(r)

	if err := sut.tmpl.Execute(sut.buf, sut.tctx); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	data := sut.buf.Bytes()
	var res *commonmodels.ColumnRawValue
	if len(data) == 2 && data[0] == '\\' && data[1] == 'N' {
		res = commonmodels.NewColumnRawValue(nil, true)
	} else {
		if sut.validate {
			if _, err := r.TableDriver().DecodeValueByColumnIdx(sut.columnIdx, data); err != nil {
				return fmt.Errorf("validate template output via driver: %w", err)
			}
		}
		res = commonmodels.NewColumnRawValue(data, false)
	}
	if err := r.SetRawColumnValueByIdx(sut.columnIdx, res); err != nil {
		return fmt.Errorf("set raw value: %w", err)
	}

	sut.buf.Reset()
	return nil
}

type ColumnContext struct {
	columnType string
	columnName string
	*gmtemplate.RecordContextReadOnly
}

func NewColumnContext(columnType string, columnName string) *ColumnContext {
	return &ColumnContext{
		columnType:            columnType,
		columnName:            columnName,
		RecordContextReadOnly: gmtemplate.NewRecordContextReadOnly(),
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
