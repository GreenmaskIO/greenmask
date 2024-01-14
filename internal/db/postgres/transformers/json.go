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

// TODO:
//		* Add implicit keys obfuscation. For instance we have json {"a": {"b": 1}} you could assign "b" as path
//	      instead of "a.b"
//		* Add behaviour - raise error or skip when path is not found
//		* Add template value
//		* Add obfuscation of the rest keys for instance - delete not changed keys or set default value (null)

package transformers

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"text/template"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	templateToolkit "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils/template"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	JsonDeleteOpName = "delete"
	JsonSetOpName    = "set"
)

var JsonTransformerDefinition = utils.NewTransformerDefinition(

	utils.NewTransformerProperties(
		"Json",
		"Update json document",
	),

	NewJsonTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("json", "jsonb"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"operations",
		`list of operations that contains editing operation [{"operation": "set|delete", "path": "path to the part of the document", "value": "value in any type - string, int, float, list, object, null", "value_template": "go template", "error_not_exist", "raise error if not exists - boolean"}]`,
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"keep_null",
		"apply changes in value is null",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

var jsonSetOpt = &sjson.Options{
	ReplaceInPlace: true,
}

type Operation struct {
	Operation     string      `mapstructure:"operation"`
	Value         interface{} `mapstructure:"value,omitempty"`
	ValueTemplate string      `mapstructure:"value_template,omitempty"`
	Path          string      `mapstructure:"path"`
	ErrorNotExist bool        `mapstructure:"error_not_exist"`
	tmpl          *template.Template
}

func (o *Operation) Apply(inp []byte, tctx *JsonContext, buf *bytes.Buffer) ([]byte, error) {
	var res []byte
	var err error

	switch o.Operation {
	case JsonSetOpName:
		if o.tmpl != nil {
			buf.Reset()
			tctx.setValue(inp, o.Path)
			if o.ErrorNotExist && !tctx.exists {
				return nil, fmt.Errorf("value by path \"%s\" does not exist", o.Path)
			}
			if err = o.tmpl.Execute(buf, tctx); err != nil {
				return nil, fmt.Errorf("error executing template: %w", err)
			}
			newValue := buf.Bytes()
			res, err = sjson.SetRawBytesOptions(inp, o.Path, newValue, jsonSetOpt)
			if err != nil {
				return nil, fmt.Errorf("error applying set raw operation: %w", err)
			}
		} else {
			res, err = sjson.SetBytesOptions(inp, o.Path, o.Value, jsonSetOpt)
			if err != nil {
				return nil, fmt.Errorf("error applying set operation: %w", err)
			}
		}

	case JsonDeleteOpName:
		if o.ErrorNotExist && !gjson.GetBytes(inp, o.Path).Exists() {
			return nil, fmt.Errorf("value by path \"%s\" does not exist", o.Path)
		}
		res, err = sjson.DeleteBytes(inp, o.Path)
		if err != nil {
			return nil, fmt.Errorf("error applying delete operation: %w", err)
		}

	default:
		return nil, fmt.Errorf("unknown operation %s", o.Operation)
	}

	return res, nil
}

type JsonTransformer struct {
	columnName      string
	columnIdx       int
	operations      []*Operation
	affectedColumns map[int]string
	tctx            *JsonContext
	buf             *bytes.Buffer
	keepNull        bool
}

func NewJsonTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var ops []*Operation
	var columnName string
	var keepNull bool

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["operations"]
	if _, err := p.Scan(&ops); err != nil {
		return nil, nil, fmt.Errorf("unable to parse operations param: %w", err)
	}

	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	for idx, o := range ops {
		if o.ValueTemplate != "" {
			tmpl, err := template.New(fmt.Sprintf("op[%d] %s %s", idx, o.Operation, o.Path)).
				Funcs(templateToolkit.FuncMap()).
				Parse(o.ValueTemplate)
			if err != nil {
				return nil, nil, fmt.Errorf("error parsing template op[%d] with path \"%s\": %w", idx, o.Path, err)
			}
			o.tmpl = tmpl
		}
	}

	return &JsonTransformer{
		columnName:      columnName,
		operations:      ops,
		columnIdx:       idx,
		affectedColumns: affectedColumns,
		buf:             bytes.NewBuffer(nil),
		tctx:            NewJsonContext(),
		keepNull:        keepNull,
	}, nil, nil
}

func (jt *JsonTransformer) GetAffectedColumns() map[int]string {
	return jt.affectedColumns
}

func (jt *JsonTransformer) Init(ctx context.Context) error {
	return nil
}

func (jt *JsonTransformer) Done(ctx context.Context) error {
	return nil
}

func (jt *JsonTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	v, err := r.GetRawColumnValueByIdx(jt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("cannot scan column value: %w", err)
	}
	if v.IsNull && jt.keepNull {
		return r, nil
	}
	jt.tctx.setRecord(r)

	res := slices.Clone(v.Data)
	for idx, op := range jt.operations {
		jt.buf.Reset()
		res, err = op.Apply(res, jt.tctx, jt.buf)
		if err != nil {
			return nil, fmt.Errorf("cannot apply \"%s\" operation[%d] with path %s: %w", op.Operation, idx, op.Path, err)
		}
	}

	if err = r.SetRawColumnValueByIdx(jt.columnIdx, toolkit.NewRawValue(res, false)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(JsonTransformerDefinition)
}
