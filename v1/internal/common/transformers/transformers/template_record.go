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
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	gmtemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const TransformerNameTemplateRecord = "TemplateRecord"

var TemplateRecordTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameTemplateRecord,
		"Modify the record using gotemplate",
	),

	NewTemplateRecordTransformer,

	commonparameters.MustNewParameterDefinition(
		"template",
		"gotemplate string",
	).SetRequired(true),
)

type TemplateRecordTransformer struct {
	template        string
	affectedColumns map[int]string
	tmpl            *template.Template
	buf             *bytes.Buffer
	tctx            *gmtemplate.RecordContextReadWrite
	columns         []string
}

func NewTemplateRecordTransformer(
	_ context.Context,
	_ commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	var templateStr string
	var columns []string
	affectedColumns := make(map[int]string)
	p := parameters["template"]
	if err := p.Scan(&templateStr); err != nil {
		return nil, fmt.Errorf("unable to scan \"template\" param: %w", err)
	}

	t := template.New("tmpl").Funcs(gmtemplate.FuncMap())
	tmpl, err := t.Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing template: %w", err)
	}

	return &TemplateRecordTransformer{
		template:        templateStr,
		affectedColumns: affectedColumns,
		tmpl:            tmpl,
		buf:             bytes.NewBuffer(nil),
		tctx:            gmtemplate.NewRecordContextReadWrite(),
		columns:         columns,
	}, nil
}

func (t *TemplateRecordTransformer) GetAffectedColumns() map[int]string {
	return map[int]string{}
}

func (t *TemplateRecordTransformer) Init(context.Context) error {
	return nil
}

func (t *TemplateRecordTransformer) Done(context.Context) error {
	return nil
}

func (t *TemplateRecordTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	t.tctx.SetRecord(r)

	if err := t.tmpl.Execute(t.buf, t.tctx); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	t.buf.Reset()
	t.tctx.Clean()
	return nil
}

func (t *TemplateRecordTransformer) Describe() string {
	return TransformerNameTemplateRecord
}
