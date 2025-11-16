// Copyright 2025 Greenmask
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
	"errors"
	"fmt"
	"slices"
	"text/template"

	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators/transformers"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	gmtemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const (
	engineModeRandom = iota
	engineModeDeterministic
)

const randomPersonAnyGender = "Any"

const TransformerNameRandomPerson = "RandomPerson"

type randomPersonColumns struct {
	Name      string `json:"name"`
	Template  string `json:"template"`
	Hashing   bool   `json:"hashing"`
	KeepNull  *bool  `json:"keep_null"`
	HashOnly  bool   `json:"hash_only"`
	tmpl      *template.Template
	columnIdx int
}

func (cc *randomPersonColumns) IsAffected() bool {
	return true
}

func (cc *randomPersonColumns) ColumnName() string {
	return cc.Name
}

var errUnableToMapGender = errors.New("unable to map gender")

var RandomPersonTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameRandomPerson,
		"Generate random person data (Title, FirstName, LastName, Gender)",
	),

	NewRandomNameTransformer,

	commonparameters.MustNewParameterDefinition(
		"columns",
		"column names and templates to fill with random person data. See documentation for details.",
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
						var columns []*randomPersonColumns
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

	commonparameters.MustNewParameterDefinition(
		"gender",
		"set specific gender (possible values: Male, Female, Any)",
	).SetDynamicMode(
		commonparameters.NewDynamicModeProperties().
			SetColumnProperties(
				commonparameters.NewColumnProperties().
					SetAllowedColumnTypeClasses(commonmodels.TypeClassText),
			),
	).SetDefaultValue(commonmodels.ParamsValue("Any")),

	commonparameters.MustNewParameterDefinition(
		"gender_mapping",
		"Specify gender name to possible values when using dynamic mode in \"gender\" parameter",
	).SetDefaultValue(commonmodels.ParamsValue(`{"Male": ["male", "M", "m", "man", "Man"], "Female": ["female", "F", "f", "w", "woman", "Woman"]}`)),

	commonparameters.MustNewParameterDefinition(
		"database",
		"Database of available names it must be map",
	),

	// TODO: Allow user to override the default names, surnames and genders with kind of dictionary

	defaultEngineParameterDefinition,
)

type RandomNameTransformer struct {
	t               *transformers.RandomPersonTransformer
	columns         []*randomPersonColumns
	gender          string
	affectedColumns map[int]string
	dynamicMode     bool
	genderMapping   map[string]string
	genderParam     commonparameters.Parameterizer
	// originalData is used to store original data for hash engine for further hashing
	originalData []byte
	engine       int
	buf          *bytes.Buffer
	nullableMap  map[int]bool
}

func NewRandomNameTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	var columns []*randomPersonColumns
	gender := transformers.AnyGenderName
	reverseGenderMapping := make(map[string]string)
	genderParam := parameters["gender"]

	dynamicMode := isInDynamicMode(parameters)

	columns, affectedColumns, err := getColumnContainerParameter[*randomPersonColumns](
		ctx, tableDriver, parameters, "columns",
	)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	if err := validateRandomPersonColumnsAndSetDefault(ctx, tableDriver, columns, engineModeRandom); err != nil {
		return nil, fmt.Errorf("validate \"columns\" parameter: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	var engineMode int
	switch engine {
	case EngineParameterValueRandom:
		engineMode = engineModeRandom
	case EngineParameterValueDeterministic, EngineParameterValueHash:
		engineMode = engineModeDeterministic
	}

	db, err := getParameterValueWithNameAndDefault[transformers.Database](ctx, parameters, "database", transformers.DefaultPersonMap)
	if err != nil {
		return nil, fmt.Errorf("get \"database\" parameter: %w", err)
	}

	t := transformers.NewRandomPersonTransformer(gender, db)
	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("unable to set generator: %w", err)
	}

	if !dynamicMode {
		gender, err = getParameterValueWithName[string](ctx, parameters, "gender")
		if err != nil {
			return nil, fmt.Errorf("unable to scan \"gender\" parameter: %w", err)
		}
	}

	if gender != "" {
		if err := randomNameTransformerValidateGender(ctx, gender, t.GetDb().Genders); err != nil {
			return nil, fmt.Errorf("validate \"gender\" parameter: %w", err)
		}
	}

	genderMapping, err := getParameterValueWithName[map[string][]string](ctx, parameters, "gender_mapping")
	if err != nil {
		return nil, fmt.Errorf("get \"gender_mapping\" parameter: %w", err)
	}

	// generate reverse mapping for faster access
	for k, v := range genderMapping {
		if err := randomNameTransformerValidateGender(
			validationcollector.WithMeta(ctx, "MappingKey", k), k, t.GetDb().Genders,
		); err != nil {
			return nil, fmt.Errorf("validate \"gender_mapping\" parameter: %w", err)
		}
		for _, val := range v {
			reverseGenderMapping[val] = k
		}
	}

	return &RandomNameTransformer{
		t:               t,
		gender:          gender,
		genderMapping:   reverseGenderMapping,
		columns:         columns,
		genderParam:     genderParam,
		affectedColumns: affectedColumns,
		dynamicMode:     dynamicMode,
		originalData:    make([]byte, 256),
		engine:          engineMode,
		buf:             bytes.NewBuffer(nil),
		nullableMap:     make(map[int]bool, len(columns)),
	}, nil
}

func (t *RandomNameTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *RandomNameTransformer) Init(context.Context) error {
	return nil
}

func (t *RandomNameTransformer) Done(context.Context) error {
	return nil
}

func (t *RandomNameTransformer) Transform(ctx context.Context, r commonininterfaces.Recorder) error {
	gender := t.gender
	if t.dynamicMode {
		if err := t.genderParam.Scan(&gender); err != nil {
			return fmt.Errorf("scan \"gender\" parameter in dynamic mode: %w", err)
		}
		var ok bool
		gender, ok = t.genderMapping[gender]
		if !ok {
			log.Ctx(ctx).Debug().
				Str("DynamiValue", gender).
				Err(errUnableToMapGender)
			return errUnableToMapGender
		}
	}

	// if we are in hash engine mode, we need to clear buffer before filling it with new data
	t.originalData = t.originalData[:0]
	for _, c := range t.columns {
		// In this cycle we need to get the raw values for each column
		// and check if it is null or not.
		rawVal, err := r.GetRawColumnValueByIdx(c.columnIdx)
		if err != nil {
			return fmt.Errorf("get raw value by idx %d: %w", c.columnIdx, err)
		}
		t.nullableMap[c.columnIdx] = rawVal.IsNull
		// we need to hash only columns that are marked for hashing
		if t.engine == engineModeDeterministic {
			// This part is required only for hash engine mode.
			if !c.Hashing {
				// If column is not marked for hashing, we skip it.
				continue
			}
			if !rawVal.IsNull {
				t.originalData = append(t.originalData, rawVal.Data...)
			}
		}
	}

	nameAttrs, err := t.t.GetFullName(gender, t.originalData)
	if err != nil {
		return fmt.Errorf("generate name: %w", err)
	}

	for _, c := range t.columns {
		if c.HashOnly {
			// Skip not affected columns, they can be used for hashing only.
			continue
		}
		if t.nullableMap[c.columnIdx] && c.KeepNull != nil && *c.KeepNull {
			continue
		}
		t.buf.Reset()
		err = c.tmpl.Execute(t.buf, nameAttrs)
		if err != nil {
			return fmt.Errorf("execute template for column %s: %w", c.Name, err)
		}
		newRawVal := commonmodels.NewColumnRawValue(slices.Clone(t.buf.Bytes()), false)
		if err = r.SetRawColumnValueByIdx(c.columnIdx, newRawVal); err != nil {
			return fmt.Errorf("set new value for column \"%s\": %w", c.Name, err)
		}
	}
	return nil
}

func (t *RandomNameTransformer) Describe() string {
	return TransformerNameRandomPerson
}

func randomNameTransformerValidateGender(
	ctx context.Context, gender string, genders []string,
) error {
	if !slices.Contains(genders, gender) && gender != randomPersonAnyGender {
		validationcollector.FromContext(ctx).
			Add(commonmodels.NewValidationWarning().
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("ParameterValue", gender).
				AddMeta("AllowedValues", append(append([]string{}, genders...), randomPersonAnyGender)).
				SetMsg("wrong gender name"))
		return commonmodels.ErrFatalValidationError
	}
	return nil
}

func validateRandomPersonColumnsAndSetDefault(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	columns []*randomPersonColumns,
	engineMode int,
) error {
	var hasHashingColumns bool

	for idx, c := range columns {
		column, err := tableDriver.GetColumnByName(c.Name)
		if err != nil {
			return fmt.Errorf("get column by name: %w", err)
		}
		c.columnIdx = column.Idx
		if c.HashOnly {
			log.Ctx(ctx).Debug().
				Str("ColumnName", c.Name).
				Msg("skipping validation for \"hash_only\" column")
			continue
		}
		if c.Template == "" {
			validationcollector.FromContext(ctx).
				Add(commonmodels.NewValidationWarning().
					SetSeverity(commonmodels.ValidationSeverityError).
					AddMeta("ParameterName", "columns").
					AddMeta("ListIdx", idx).
					SetMsg("\"template\" parameters is required: received empty"))
			return commonmodels.ErrFatalValidationError
		}

		if c.Template != "" {
			tmpl, err := template.New(c.Name).
				Funcs(gmtemplate.FuncMap()).
				Parse(c.Template)
			if err != nil {
				validationcollector.FromContext(ctx).
					Add(commonmodels.NewValidationWarning().
						SetSeverity(commonmodels.ValidationSeverityError).
						AddMeta("Error", err.Error()).
						AddMeta("ParameterName", "columns").
						AddMeta("ListIdx", idx).
						SetMsg("error parsing template"))
				return commonmodels.ErrFatalValidationError
			}
			c.tmpl = tmpl
		}

		if c.KeepNull == nil {
			defaultKeepNullValue := true
			c.KeepNull = &defaultKeepNullValue
		}

		// Do we need to calculate hash for this column?
		if c.Hashing {
			hasHashingColumns = true
		}
	}

	if !hasHashingColumns && engineMode == engineModeDeterministic {
		for _, c := range columns {
			log.Ctx(ctx).Debug().
				Str("ColumnName", c.Name).
				Msg("no columns marked for hashing, marking all columns for hashing")
			c.Hashing = true
		}
	}

	return nil
}
