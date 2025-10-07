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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerstemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const (
	jsonDeleteOpName = "delete"
	jsonSetOpName    = "set"
)

const JsonTransformerName = "Json"

var errInvalidJson = fmt.Errorf("invalid json data")

var JsonTransformerDefinition = transformerutils.NewTransformerDefinition(

	transformerutils.NewTransformerProperties(
		JsonTransformerName,
		"Update json document",
	),

	NewJsonTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("json", "jsonb", "text"),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"operations",
		`list of operations that contains editing operation [{"operation": "set|delete", "path": "path to the part of the document", "value": "value in any type - string, int, float, list, object, null", "value_template": "go template", "error_not_exist", "raise error if not exists - boolean"}]`,
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"skip_invalid_json",
		"skip invalid json objects instead of raising error",
	).SetDefaultValue(commonmodels.ParamsValue("false")),

	defaultKeepNullParameterDefinition,
)

var jsonSetOpt = &sjson.Options{
	ReplaceInPlace: true,
}

type Operation struct {
	Operation     string      `mapstructure:"operation" json:"operation"`
	Value         interface{} `mapstructure:"value,omitempty" json:"value"`
	ValueTemplate string      `mapstructure:"value_template,omitempty" json:"value_template"`
	Path          string      `mapstructure:"path" json:"path"`
	ErrorNotExist bool        `mapstructure:"error_not_exist" json:"error_not_exist"`
	tmpl          *template.Template
}

func (o *Operation) Apply(inp []byte, jctx *JsonContext, buf *bytes.Buffer) ([]byte, error) {
	var res []byte
	var err error

	switch o.Operation {
	case jsonSetOpName:
		if o.tmpl != nil {
			buf.Reset()
			jctx.setValue(inp, o.Path)
			if o.ErrorNotExist && !jctx.exists {
				return nil, fmt.Errorf("value by path \"%s\" does not exist", o.Path)
			}
			if err = o.tmpl.Execute(buf, jctx); err != nil {
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

	case jsonDeleteOpName:
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
	skipInvalidJson bool
}

func NewJsonTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("error getting column parameter: %w", err)
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, "keep_null")
	if err != nil {
		return nil, fmt.Errorf("unable to scan \"keep_null\" parameter: %w", err)
	}

	skipInvalidJson, err := getParameterValueWithName[bool](ctx, parameters, "skip_invalid_json")
	if err != nil {
		return nil, fmt.Errorf("unable to scan \"skip_invalid_json\" parameter: %w", err)
	}

	ops, err := getParameterValueWithName[[]*Operation](ctx, parameters, "operations")
	if err != nil {
		return nil, fmt.Errorf("unable to scan \"function\" parameter: %w", err)
	}

	for idx, o := range ops {
		if o.ValueTemplate != "" {
			tmpl, err := template.New(fmt.Sprintf("op[%d] %s %s", idx, o.Operation, o.Path)).
				Funcs(transformerstemplate.FuncMap()).
				Parse(o.ValueTemplate)
			if err != nil {
				return nil, fmt.Errorf("error parsing template op[%d] with path \"%s\": %w", idx, o.Path, err)
			}
			o.tmpl = tmpl
		}
	}

	return &JsonTransformer{
		columnName: columnName,
		operations: ops,
		columnIdx:  column.Idx,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		buf:             bytes.NewBuffer(nil),
		tctx:            NewJsonContext(),
		keepNull:        keepNull,
		skipInvalidJson: skipInvalidJson,
	}, nil
}

func (jt *JsonTransformer) GetAffectedColumns() map[int]string {
	return jt.affectedColumns
}

func (jt *JsonTransformer) Init(context.Context) error {
	return nil
}

func (jt *JsonTransformer) Done(context.Context) error {
	return nil
}

func (jt *JsonTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	v, err := r.GetRawColumnValueByIdx(jt.columnIdx)
	if err != nil {
		return fmt.Errorf("cannot scan column value: %w", err)
	}
	if v.IsNull && jt.keepNull {
		return nil
	}
	if !gjson.ValidBytes(v.Data) {
		if jt.skipInvalidJson {
			return nil
		}
		return errInvalidJson
	}

	jt.tctx.setRecord(r)

	res := slices.Clone(v.Data)
	for idx, op := range jt.operations {
		jt.buf.Reset()
		res, err = op.Apply(res, jt.tctx, jt.buf)
		if err != nil {
			return fmt.Errorf("cannot apply \"%s\" operation[%d] with path %s: %w", op.Operation, idx, op.Path, err)
		}
	}

	if err = r.SetRawColumnValueByIdx(jt.columnIdx, commonmodels.NewColumnRawValue(res, false)); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}

	return nil
}
