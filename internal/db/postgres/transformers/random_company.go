package transformers

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"text/template"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const RandomCompanyTransformerName = "RandomCompany"

var randomCompanyTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomCompanyTransformerName,
		"Generate random company data (CompanyName, CompanySuffix)",
	),

	NewRandomCompanyTransformer,

	toolkit.MustNewParameterDefinition(
		"columns",
		"columns name",
	).SetRequired(true),

	engineParameterDefinition,
)

type randomCompanyNameColumns struct {
	Name      string `json:"name"`
	Template  string `json:"template"`
	Hashing   bool   `json:"hashing"`
	KeepNull  *bool  `json:"keep_null"`
	tmpl      *template.Template
	columnIdx int
}

type RandomCompanyTransformer struct {
	t               *transformers.RandomCompanyTransformer
	columns         []*randomCompanyNameColumns
	affectedColumns map[int]string
	dynamicMode     bool
	originalData    []byte
	engine          int
	buf             *bytes.Buffer
	nullableMap     map[int]bool
}

func NewRandomCompanyTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var engine string
	var dynamicMode bool
	var columns []*randomCompanyNameColumns
	var warns toolkit.ValidationWarnings

	columnsParam := parameters["columns"]
	engineParam := parameters["engine"]

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

	t := transformers.NewRandomCompanyTransformer(nil)

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

	affectedColumns, warns := validateCompanyColumnsAndSetDefault(driver, columns, engineMode, attributes)
	if warns.IsFatal() {
		return nil, warns, nil
	}

	return &RandomCompanyTransformer{
		t:               t,
		columns:         columns,
		affectedColumns: affectedColumns,
		dynamicMode:     dynamicMode,
		originalData:    make([]byte, 256),
		engine:          engineMode,
		buf:             bytes.NewBuffer(nil),
		nullableMap:     make(map[int]bool, len(columns)),
	}, warns, nil
}

func (nft *RandomCompanyTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *RandomCompanyTransformer) Init(ctx context.Context) error {
	return nil
}

func (nft *RandomCompanyTransformer) Done(ctx context.Context) error {
	return nil
}

func (nft *RandomCompanyTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
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

	nameAttrs, err := nft.t.GetCompanyName(nft.originalData)
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

func validateCompanyColumnsAndSetDefault(driver *toolkit.Driver, columns []*randomCompanyNameColumns, engineMode int, attributes []string) (map[int]string, toolkit.ValidationWarnings) {
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
	utils.DefaultTransformerRegistry.MustRegister(randomCompanyTransformerDefinition)
}
