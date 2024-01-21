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
	"slices"
	"text/template"

	"github.com/go-faker/faker/v4"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RealAddressTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RealAddress",
		"Generate a real address",
	),

	NewRealAddressTransformer,

	toolkit.MustNewParameter(
		"columns",
		"affected column names."+
			"The structure:"+
			`{`+
			`"name": "type:string, required:true, description: column Name",`+
			`"template": "type:string, required:true, description: gotemplate with real address attributes injections",`+
			`"keep_null": "type:bool, required:false, description: keep null values",`+
			`}`,
	).SetRequired(true),
)

type RealAddressTransformer struct {
	columns         []*RealAddressColumn
	affectedColumns map[int]string
	buf             *bytes.Buffer
}

type RealAddressColumn struct {
	Name      string `json:"name"`
	KeepNull  bool   `json:"keep_null"`
	Template  string `json:"template"`
	columnIdx int
	tmpl      *template.Template
}

type RealAddressValue struct {
	Address    string  `json:"address1"`
	City       string  `json:"city"`
	State      string  `json:"state"`
	PostalCode string  `json:"postalCode"`
	Latitude   float64 `json:"lat"`
	Longitude  float64 `json:"lng"`
}

func NewRealAddressTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings
	var columns []*RealAddressColumn

	p := parameters["columns"]
	if _, err := p.Scan(&columns); err != nil {
		return nil, nil, err
	}

	affectedColumns := make(map[int]string)

	testBuf := bytes.NewBuffer(nil)
	for _, col := range columns {
		idx, _, ok := driver.GetColumnByName(col.Name)
		if !ok {
			return nil, nil, fmt.Errorf("column with name %s is not found", col.Name)
		}
		col.columnIdx = idx // Set the correct column index
		affectedColumns[idx] = col.Name

		if col.Template == "" {
			warnings = append(warnings,
				toolkit.NewValidationWarning().
					SetMsg("template value must not be empty").
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("ColumnName", col.Name).
					AddMeta("ParameterName", "columns"),
			)
			continue
		}

		tmpl, err := template.New("").Parse(col.Template)
		if err != nil {
			warnings = append(warnings,
				toolkit.NewValidationWarning().
					SetMsg("error parsing template").
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("TemplateString", col.Template).
					AddMeta("ColumnName", col.Name).
					AddMeta("ParameterName", "columns").
					AddMeta("Error", err.Error()),
			)
			continue
		}

		testAddress := getRealAddress()
		if err = tmpl.Execute(testBuf, testAddress); err != nil {
			warnings = append(warnings,
				toolkit.NewValidationWarning().
					SetMsg("error validating template").
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("TemplateString", col.Template).
					AddMeta("ColumnName", col.Name).
					AddMeta("ParameterName", "columns").
					AddMeta("Error", err.Error()),
			)
		}
		col.tmpl = tmpl
	}

	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	return &RealAddressTransformer{
		columns:         columns,
		affectedColumns: affectedColumns,
		buf:             bytes.NewBuffer(nil),
	}, warnings, nil
}

func (rat *RealAddressTransformer) GetAffectedColumns() map[int]string {
	return rat.affectedColumns
}

func (rat *RealAddressTransformer) Init(ctx context.Context) error {
	return nil
}

func (rat *RealAddressTransformer) Done(ctx context.Context) error {
	return nil
}

func (rat *RealAddressTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	address := getRealAddress()

	// Iterate over the columns and update the record with generated address data
	for _, col := range rat.columns {
		rawValue, err := r.GetRawColumnValueByIdx(col.columnIdx)
		if err != nil {
			return nil, err
		}
		if rawValue.IsNull && col.KeepNull {
			return r, nil
		}

		rat.buf.Reset()
		if err = col.tmpl.Execute(rat.buf, address); err != nil {
			return nil, fmt.Errorf("error executing template for column \"%s\": %w", col.Name, err)
		}

		newRawValue := toolkit.NewRawValue(slices.Clone(rat.buf.Bytes()), false)

		// Update the record for the current column with the generated value
		if err := r.SetRawColumnValueByIdx(col.columnIdx, newRawValue); err != nil {
			return nil, fmt.Errorf("unable to set new value: %w", err)
		}
	}

	return r, nil
}

func getRealAddress() *RealAddressValue {
	addr := faker.GetRealAddress()

	return &RealAddressValue{
		Address:    addr.Address,
		City:       addr.City,
		State:      addr.State,
		PostalCode: addr.PostalCode,
		Latitude:   addr.Coordinates.Latitude,
		Longitude:  addr.Coordinates.Longitude,
	}
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RealAddressTransformerDefinition)
}
