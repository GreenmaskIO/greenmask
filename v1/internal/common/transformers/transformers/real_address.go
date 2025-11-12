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
	"encoding/json"
	"fmt"
	"slices"
	"text/template"

	"github.com/go-faker/faker/v4"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const TransformerNameRealAddress = "RealAddress"

var RealAddressTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameRealAddress,
		"Generate a real address",
	),

	NewRealAddressTransformer,

	commonparameters.MustNewParameterDefinition(
		"columns",
		"affected column names."+
			"The structure:"+
			`{`+
			`"name": "type:string, required:true, description: column Name",`+
			`"template": "type:string, required:true, description: gotemplate with real address attributes injections",`+
			`"keep_null": "type:bool, required:false, description: keep null values",`+
			`}`,
	).SetRequired(true).
		SetColumnContainer(
			commonparameters.NewColumnContainerProperties().
				SetColumnProperties(
					commonparameters.NewColumnProperties().
						SetAllowedColumnTypeClasses(commonmodels.TypeClassText),
				).
				SetUnmarshaler(
					func(_ context.Context, _ *commonparameters.ParameterDefinition, data commonmodels.ParamsValue) (
						[]commonparameters.ColumnContainer, error,
					) {
						var columns []*realAddressColumn
						if err := json.Unmarshal(data, &columns); err != nil {
							return nil, fmt.Errorf("unmarshal columns parameter: %w", err)
						}
						cc := make([]commonparameters.ColumnContainer, len(columns))
						for i := range columns {
							cc[i] = columns[i]
						}
						return cc, nil
					},
				),
		),
)

type realAddressColumn struct {
	Name      string `json:"name"`
	KeepNull  bool   `json:"keep_null"`
	Template  string `json:"template"`
	columnIdx int
	tmpl      *template.Template
}

func (r realAddressColumn) IsAffected() bool {
	return true
}

func (r realAddressColumn) ColumnName() string {
	return r.Name
}

type realAddressValue struct {
	Address    string  `json:"address1"`
	City       string  `json:"city"`
	State      string  `json:"state"`
	PostalCode string  `json:"postalCode"`
	Latitude   float64 `json:"lat"`
	Longitude  float64 `json:"lng"`
}

type RealAddressTransformer struct {
	columns         []*realAddressColumn
	affectedColumns map[int]string
	buf             *bytes.Buffer
}

func NewRealAddressTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columns, affectedColumns, err := getColumnContainerParameter[*realAddressColumn](
		ctx, tableDriver, parameters, "columns",
	)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	testBuf := bytes.NewBuffer(nil)
	for _, col := range columns {
		column, err := tableDriver.GetColumnByName(col.Name)
		if err != nil {
			return nil, fmt.Errorf("column with name %s is not found", col.Name)
		}
		col.columnIdx = column.Idx

		if col.Template == "" {
			validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
				SetMsg("template value must not be empty").
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("ColumnName", col.Name).
				AddMeta("ParameterName", "columns"))
			return nil, fmt.Errorf(
				"template value must not be empty for column %s",
				commonmodels.ErrFatalValidationError,
			)
		}

		tmpl, err := template.New("").Parse(col.Template)
		if err != nil {
			validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
				SetMsg("error parsing template").
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("TemplateString", col.Template).
				AddMeta("ColumnName", col.Name).
				AddMeta("ParameterName", "columns").
				AddMeta("Error", err.Error()))
			return nil, fmt.Errorf(
				"error parsing template for column %s: %w",
				col.Name,
				commonmodels.ErrFatalValidationError,
			)
		}

		testAddress := getRealAddress()
		if err = tmpl.Execute(testBuf, testAddress); err != nil {
			validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
				SetMsg("error validating template").
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("TemplateString", col.Template).
				AddMeta("ColumnName", col.Name).
				AddMeta("ParameterName", "columns").
				AddMeta("Error", err.Error()))
			return nil, fmt.Errorf(
				"error validating template for column %s: %w",
				col.Name,
				commonmodels.ErrFatalValidationError,
			)
		}
		col.tmpl = tmpl
	}

	return &RealAddressTransformer{
		columns:         columns,
		affectedColumns: affectedColumns,
		buf:             bytes.NewBuffer(nil),
	}, nil
}

func (t *RealAddressTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *RealAddressTransformer) Init(context.Context) error {
	return nil
}

func (t *RealAddressTransformer) Done(context.Context) error {
	return nil
}

func (t *RealAddressTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	address := getRealAddress()

	// Iterate over the columns and update the record with generated address data
	for _, col := range t.columns {
		rawValue, err := r.GetRawColumnValueByIdx(col.columnIdx)
		if err != nil {
			return fmt.Errorf("get raw column value: %w", err)
		}
		if rawValue.IsNull && col.KeepNull {
			return nil
		}

		t.buf.Reset()
		if err = col.tmpl.Execute(t.buf, address); err != nil {
			return fmt.Errorf("execute template for column \"%s\": %w", col.Name, err)
		}

		newRawValue := commonmodels.NewColumnRawValue(slices.Clone(t.buf.Bytes()), false)

		// Update the record for the current column with the generated value
		if err := r.SetRawColumnValueByIdx(col.columnIdx, newRawValue); err != nil {
			return fmt.Errorf("set new value: %w", err)
		}
	}

	return nil
}

func (t *RealAddressTransformer) Describe() string {
	return TransformerNameRealAddress
}

func getRealAddress() *realAddressValue {
	addr := faker.GetRealAddress()

	return &realAddressValue{
		Address:    addr.Address,
		City:       addr.City,
		State:      addr.State,
		PostalCode: addr.PostalCode,
		Latitude:   addr.Coordinates.Latitude,
		Longitude:  addr.Coordinates.Longitude,
	}
}
