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
	templateToolkit "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils/template"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var TemplateTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"Template",
		"Modify the value using gotemplate",
	),
	NewTemplateTransformer,
	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"template",
		"gotemplate string",
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"validate",
		"validate template result via PostgreSQL driver decoding",
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("false")),
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

func NewTemplateTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, templateStr string

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"column\" param: %w", err)
	}

	idx, c, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["template"]
	if _, err := p.Scan(&templateStr); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"templateStr\" param: %w", err)
	}

	t := template.New("tmpl").Funcs(templateToolkit.FuncMap())
	tmpl, err := t.Parse(templateStr)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing template: %w", err)
	}

	return &TemplateTransformer{
		columnName:      columnName,
		template:        templateStr,
		columnIdx:       idx,
		affectedColumns: affectedColumns,
		tmpl:            tmpl,
		buf:             bytes.NewBuffer(nil),
		tctx:            NewColumnContext(c.TypeName, columnName),
	}, nil, nil
}

func (sut *TemplateTransformer) GetAffectedColumns() map[int]string {
	return sut.affectedColumns
}

func (sut *TemplateTransformer) Init(ctx context.Context) error {
	return nil
}

func (sut *TemplateTransformer) Done(ctx context.Context) error {
	return nil
}

func (sut *TemplateTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	sut.tctx.setRecord(r)

	if err := sut.tmpl.Execute(sut.buf, sut.tctx); err != nil {
		return nil, fmt.Errorf("error executing template: %w", err)
	}

	data := sut.buf.Bytes()
	var res *toolkit.RawValue
	if len(data) == 2 && data[0] == '\\' && data[1] == 'N' {
		res = toolkit.NewRawValue(nil, true)
	} else {
		if sut.validate {
			if _, err := r.Driver.DecodeValueByColumnIdx(sut.columnIdx, res.Data); err != nil {
				return nil, fmt.Errorf("error validating template output via driver: %w", err)
			}
		}
		res = toolkit.NewRawValue(data, false)
	}
	if err := r.SetRawColumnValueByIdx(sut.columnIdx, res); err != nil {
		return nil, fmt.Errorf("error setting raw value: %w", err)
	}

	sut.buf.Reset()
	sut.tctx.clean()
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(TemplateTransformerDefinition)
}
