package transformers

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"text/template"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	randomEngineMode = iota
	hashEngineMode
)

const (
	randomTransformerNamePart     = "first_name"
	randomTransformerLastNamePart = "last_name"
	randomTransformerFullNamePart = "full_name"
)

var randomFullNameTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"RandomFullName",
		"Generate random full name for person. Including name, surname and sex",
	),

	NewRandomNameTransformer,

	toolkit.MustNewParameterDefinition(
		"columns",
		"columns name",
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"gender",
		"set specific gender (possible values: male, female, any)",
	).SetDynamicMode(
		toolkit.NewDynamicModeProperties().
			SetCompatibleTypes("text", "varchar", "char", "bpchar"),
	).SetRawValueValidator(randomNameTransformerValidateGender),

	toolkit.MustNewParameterDefinition(
		"gender_mapping",
		"Specify gender name to possible values when using dynamic mode in \"gender\" parameter",
	).SetDefaultValue(toolkit.ParamsValue(`{"male": ["male", "Male", "M", "m", "man", "Man"], "female": ["female", "Female", "F", "f", "w", "woman", "Woman"]}`)),

	toolkit.MustNewParameterDefinition(
		"fallback_gender",
		"Specify fallback gender if not mapped. By default any of name will be chosen",
	).SetDefaultValue(toolkit.ParamsValue("any")).
		SetRawValueValidator(randomNameTransformerValidateGender),

	// TODO: Allow user to override the default names, surnames and genders with kind of dictionary

	keepNullParameterDefinition,

	engineParameterDefinition,
)

var allowedNameParts = []string{randomTransformerNamePart, randomTransformerLastNamePart, randomTransformerFullNamePart}

type randomNameColumns struct {
	Name      string `json:"name"`
	Template  string `json:"template"`
	Hashing   bool   `json:"hashing"`
	Part      string `json:"part"`
	tmpl      *template.Template
	columnIdx int
}

type RandomNameTransformer struct {
	t               *transformers.RandomFullNameTransformer
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
	case randomEngineName:
		engineMode = randomEngineMode
	case hashEngineName:
		engineMode = hashEngineMode
	}

	if err := columnsParam.Scan(&columns); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "columns" param: %w`, err)
	}

	affectedColumns, warns := validateColumnsAndSetDefault(driver, columns, engineMode)
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

	if err := genderMappingParam.Scan(&genderMapping); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "gender_mapping" param: %w`, err)
	}
	// generate reverse mapping for faster access
	for k, v := range genderMapping {
		for _, val := range v {
			reverseGenderMapping[val] = k
		}
	}

	if err := fallbackGenderParam.Scan(&fallbackGender); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "fallback_gender" param: %w`, err)
	}

	t := transformers.NewRandomNameTransformer(gender, transformers.RandomFullNameTransformerFullNameMode)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

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
	}, nil, nil
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
		ctx = context.WithValue(ctx, "gender", gender)
	}

	// if we are in hash engine mode, we need to clear buffer before filling it with new data
	if nft.engine == hashEngineMode {
		clear(nft.originalData)
		for _, c := range nft.columns {
			// we need to hash only columns that are marked for hashing
			if !c.Hashing {
				continue
			}
			rawVal, err := r.GetRawColumnValueByIdx(c.columnIdx)
			if err != nil {
				return nil, fmt.Errorf("unable to get raw value by idx %d: %w", c.columnIdx, err)
			}
			if !rawVal.IsNull {
				nft.originalData = append(nft.originalData, rawVal.Data...)
			}
		}
	}

	nameAttrs, err := nft.t.GetFullName(ctx, nft.originalData)
	if err != nil {
		return nil, fmt.Errorf("error generating name: %w", err)
	}

	for _, c := range nft.columns {
		newRawVal := toolkit.NewRawValue(nil, false)
		if c.tmpl != nil {
			nft.buf.Reset()
			err = c.tmpl.Execute(nft.buf, nameAttrs)
			if err != nil {
				return nil, fmt.Errorf("error executing template for column %s: %w", c.Name, err)
			}
			newRawVal.Data = slices.Clone(nft.buf.Bytes())
		} else {
			switch c.Part {
			case randomTransformerNamePart:
				newRawVal.Data = []byte(nameAttrs.FirstName)
			case randomTransformerLastNamePart:
				newRawVal.Data = []byte(nameAttrs.LastName)
			case randomTransformerFullNamePart:
				newRawVal.Data = []byte(nameAttrs.FirstName + " " + nameAttrs.LastName)
			default:
				panic(fmt.Sprintf("bug in validation: unknown part %s", c.Part))
			}
		}
		if err = r.SetRawColumnValueByIdx(c.columnIdx, newRawVal); err != nil {
			return nil, fmt.Errorf("unable to set new value for column \"%s\": %w", c.Name, err)
		}
	}
	return r, nil
}

func randomNameTransformerValidateGender(p *toolkit.ParameterDefinition, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	switch string(v) {
	case transformers.MaleGenderName, transformers.FemaleGenderName, transformers.AnyGenderName:
		return nil, nil
	}
	return []*toolkit.ValidationWarning{
		toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterValue", v).
			SetMsg("wrong gender name"),
	}, nil
}

func validateColumnsAndSetDefault(driver *toolkit.Driver, columns []*randomNameColumns, engineMode int) (map[int]string, toolkit.ValidationWarnings) {
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

		if c.Part == "" && c.Template == "" {
			warns = append(warns,
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("ParameterName", "columns").
					AddMeta("ListIdx", idx).
					SetMsg("\"part\" or \"template\" parameters is required: received empty"),
			)
		}

		if c.Part != "" && c.Template != "" {
			warns = append(warns,
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("ParameterName", "columns").
					AddMeta("ListIdx", idx).
					SetMsg("part and template are mutually exclusive: received both"),
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

		if c.Part != "" && !slices.Contains(allowedNameParts, c.Part) {
			warns = append(warns,
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("ParameterName", "columns").
					AddMeta("PartValue", c.Part).
					AddMeta("ListIdx", idx).
					SetMsgf("part must be one of %s", strings.Join(allowedNameParts, ", ")),
			)
		}

		// Do we need to calculate hash for this column?
		if c.Hashing {
			hasHashingColumns = true
		}
	}

	if !hasHashingColumns && engineMode != hashEngineMode {
		for _, c := range columns {
			c.Hashing = true
		}
	}

	return affectedColumns, warns
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(randomFullNameTransformerDefinition)
}
