package transformers

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"text/template"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	randomEngineMode = iota
	hashEngineMode
)

const randomPersonAnyGender = "Any"

const RandomPersonTransformerName = "RandomPerson"

var randomPersonTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomPersonTransformerName,
		"Generate random person data (Title, FirstName, LastName, Gender)",
	),

	NewRandomNameTransformer,

	toolkit.MustNewParameterDefinition(
		"columns",
		"columns name",
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"gender",
		"set specific gender (possible values: Male, Female, Any)",
	).SetDynamicMode(
		toolkit.NewDynamicModeProperties().
			SetCompatibleTypes("text", "varchar", "char", "bpchar"),
	).SetDefaultValue(toolkit.ParamsValue("Any")),

	toolkit.MustNewParameterDefinition(
		"gender_mapping",
		"Specify gender name to possible values when using dynamic mode in \"gender\" parameter",
	).SetDefaultValue(toolkit.ParamsValue(`{"Male": ["male", "M", "m", "man", "Man"], "Female": ["female", "F", "f", "w", "woman", "Woman"]}`)),

	toolkit.MustNewParameterDefinition(
		"fallback_gender",
		"Specify fallback gender if not mapped when using dynamic mode in \"gender\" parameter",
	).SetSupportTemplate(true).
		SetDefaultValue(toolkit.ParamsValue("Any")),

	// TODO: Allow user to override the default names, surnames and genders with kind of dictionary

	engineParameterDefinition,
)

type randomNameColumns struct {
	Name      string `json:"name"`
	Template  string `json:"template"`
	Hashing   bool   `json:"hashing"`
	KeepNull  *bool  `json:"keep_null"`
	tmpl      *template.Template
	columnIdx int
}

type RandomNameTransformer struct {
	t               *transformers.RandomPersonTransformer
	columns         []*randomNameColumns
	gender          string
	fallbackGender  string
	affectedColumns map[int]string
	dynamicMode     bool
	genderMapping   map[string]string
	genderParam     toolkit.Parameterizer
	// originalData is used to store original data for hash engine for further hashing
	originalData []byte
	engine       int
	buf          *bytes.Buffer
	nullableMap  map[int]bool
}

func NewRandomNameTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var engine, fallbackGender string
	var dynamicMode bool
	var columns []*randomNameColumns
	var warns toolkit.ValidationWarnings
	genderMapping := make(map[string][]string)
	reverseGenderMapping := make(map[string]string)

	gender := transformers.AnyGenderName

	columnsParam := parameters["columns"]
	genderParam := parameters["gender"]
	genderMappingParam := parameters["gender_mapping"]
	engineParam := parameters["engine"]
	fallbackGenderParam := parameters["fallback_gender"]

	if err := engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	var engineMode int
	switch engine {
	case RandomEngineParameterName:
		engineMode = randomEngineMode
	case HashEngineParameterName:
		engineMode = hashEngineMode
	}

	t := transformers.NewRandomPersonTransformer(gender, nil)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	attributes := t.GetDb().Attributes

	if err := columnsParam.Scan(&columns); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "columns" param: %w`, err)
	}

	affectedColumns, warns := validateColumnsAndSetDefault(driver, columns, engineMode, attributes)
	if warns.IsFatal() {
		return nil, warns, nil
	}

	if genderParam.IsDynamic() {
		// if we are in dynamic mode, we will get this value from the record
		dynamicMode = true
	} else {
		if err := genderParam.Scan(&gender); err != nil {
			return nil, nil, fmt.Errorf("unable to scan \"gender\" parameter: %w", err)
		}
	}

	if gender != "" {
		warns = append(warns, randomNameTransformerValidateGender(gender, t.GetDb().Genders)...)
	}
	if warns.IsFatal() {
		return nil, warns, nil
	}

	if err := genderMappingParam.Scan(&genderMapping); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "gender_mapping" param: %w`, err)
	}
	// generate reverse mapping for faster access
	for k, v := range genderMapping {
		warns = append(warns, randomNameTransformerValidateGender(k, t.GetDb().Genders)...)
		for _, val := range v {
			reverseGenderMapping[val] = k
		}
	}
	if warns.IsFatal() {
		return nil, warns, nil
	}

	if err := fallbackGenderParam.Scan(&fallbackGender); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "fallback_gender" param: %w`, err)
	}
	warns = append(warns, randomNameTransformerValidateGender(fallbackGender, t.GetDb().Genders)...)

	return &RandomNameTransformer{
		t:               t,
		gender:          gender,
		fallbackGender:  fallbackGender,
		genderMapping:   reverseGenderMapping,
		columns:         columns,
		genderParam:     genderParam,
		affectedColumns: affectedColumns,
		dynamicMode:     dynamicMode,
		originalData:    make([]byte, 256),
		engine:          engineMode,
		buf:             bytes.NewBuffer(nil),
		nullableMap:     make(map[int]bool, len(columns)),
	}, warns, nil
}

func (nft *RandomNameTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *RandomNameTransformer) Init(ctx context.Context) error {
	return nil
}

func (nft *RandomNameTransformer) Done(ctx context.Context) error {
	return nil
}

func (nft *RandomNameTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	gender := nft.gender
	if nft.dynamicMode {
		if err := nft.genderParam.Scan(&gender); err != nil {
			return nil, fmt.Errorf("unable to scan \"gender\" parameter dynamically: %w", err)
		}
		var ok bool
		gender, ok = nft.genderMapping[gender]
		if !ok {
			gender = nft.fallbackGender
		}
	}

	// if we are in hash engine mode, we need to clear buffer before filling it with new data
	if nft.engine == hashEngineMode {
		clear(nft.originalData)
		for _, c := range nft.columns {
			rawVal, err := r.GetRawColumnValueByIdx(c.columnIdx)
			if err != nil {
				return nil, fmt.Errorf("unable to get raw value by idx %d: %w", c.columnIdx, err)
			}
			nft.nullableMap[c.columnIdx] = rawVal.IsNull
			// we need to hash only columns that are marked for hashing
			if !c.Hashing {
				continue
			}
			if !rawVal.IsNull {
				nft.originalData = append(nft.originalData, rawVal.Data...)
			}
		}
	}

	nameAttrs, err := nft.t.GetFullName(gender, nft.originalData)
	if err != nil {
		return nil, fmt.Errorf("error generating name: %w", err)
	}

	for _, c := range nft.columns {
		if nft.nullableMap[c.columnIdx] && c.KeepNull != nil && *c.KeepNull {
			continue
		}
		newRawVal := toolkit.NewRawValue(nil, false)
		nft.buf.Reset()
		err = c.tmpl.Execute(nft.buf, nameAttrs)
		if err != nil {
			return nil, fmt.Errorf("error executing template for column %s: %w", c.Name, err)
		}
		newRawVal.Data = slices.Clone(nft.buf.Bytes())
		if err = r.SetRawColumnValueByIdx(c.columnIdx, newRawVal); err != nil {
			return nil, fmt.Errorf("unable to set new value for column \"%s\": %w", c.Name, err)
		}
	}
	return r, nil
}

func randomNameTransformerValidateGender(gender string, genders []string) toolkit.ValidationWarnings {
	if !slices.Contains(genders, gender) && gender != randomPersonAnyGender {
		return []*toolkit.ValidationWarning{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("ParameterValue", gender).
				AddMeta("AllowedValues", append(append([]string{}, genders...), randomPersonAnyGender)).
				SetMsg("wrong gender name"),
		}
	}
	return nil
}

func validateColumnsAndSetDefault(driver *toolkit.Driver, columns []*randomNameColumns, engineMode int, attributes []string) (map[int]string, toolkit.ValidationWarnings) {
	affectedColumns := make(map[int]string)
	var warns toolkit.ValidationWarnings

	var hasHashingColumns bool

	for idx, c := range columns {
		if c.Name == "" {
			warns = append(warns,
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("ParameterName", "columns").
					AddMeta("ListIdx", idx).
					SetMsg("name is required"),
			)
			continue
		}

		columnIdx, _, ok := driver.GetColumnByName(c.Name)
		if !ok {
			warns = append(warns, toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("ParameterName", "columns").
				AddMeta("ParameterValue", c.Name).
				AddMeta("ListIdx", idx).
				SetMsg("column is not found"))
			continue
		}
		affectedColumns[idx] = c.Name
		c.columnIdx = columnIdx

		if c.Template == "" {
			warns = append(warns,
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("ParameterName", "columns").
					AddMeta("ListIdx", idx).
					SetMsg("\"template\" parameters is required: received empty"),
			)
		}

		if c.Template != "" {
			tmpl, err := template.New(c.Name).
				Funcs(toolkit.FuncMap()).
				Parse(c.Template)
			if err != nil {
				warns = append(warns, toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("Error", err.Error()).
					AddMeta("ParameterName", "columns").
					AddMeta("ListIdx", idx).
					SetMsg("error parsing template"),
				)
				continue
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

	if !hasHashingColumns && engineMode == hashEngineMode {
		for _, c := range columns {
			c.Hashing = true
		}
	}

	return affectedColumns, warns
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(randomPersonTransformerDefinition)
}
