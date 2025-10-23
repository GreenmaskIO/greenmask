package transformers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"text/template"

	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	gmtemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const RandomCompanyTransformerName = "RandomCompany"

var RandomCompanyTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RandomCompanyTransformerName,
		"Generate random company data (CompanyName, CompanySuffix)",
	),

	NewRandomCompanyTransformer,

	commonparameters.MustNewParameterDefinition(
		"columns",
		"columns name",
	).SetRequired(true).
		SetColumnContainer(commonparameters.NewColumnContainerProperties().
			SetAllowedTypes("text", "varchar").
			SetUnmarshaler(
				func(_ context.Context, _ *commonparameters.ParameterDefinition, data commonmodels.ParamsValue) (
					[]commonparameters.ColumnContainer, error,
				) {
					var columns []*randomCompanyNameColumns
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

	defaultEngineParameterDefinition,
)

type randomCompanyNameColumns struct {
	Name      string `json:"name"`
	Template  string `json:"template"`
	Hashing   bool   `json:"hashing"`
	KeepNull  *bool  `json:"keep_null"`
	HashOnly  bool   `json:"hash_only"`
	tmpl      *template.Template
	columnIdx int
}

func (cc *randomCompanyNameColumns) ColumnName() string {
	return cc.Name
}

type RandomCompanyTransformer struct {
	t               *transformers.RandomCompanyTransformer
	columns         []*randomCompanyNameColumns
	affectedColumns map[int]string
	originalData    []byte
	engine          int
	buf             *bytes.Buffer
	nullableMap     map[int]bool
}

func NewRandomCompanyTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columns, affectedColumns, err := getColumnContainerParameter[*randomCompanyNameColumns](
		ctx, tableDriver, parameters, "columns",
	)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	var engineMode int
	switch engine {
	case EngineParameterValueRandom:
		engineMode = randomEngineMode
	case EngineParameterValueDeterministic, EngineParameterValueHash:
		engineMode = deterministicEngineMode
	}

	t := transformers.NewRandomCompanyTransformer(nil)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	if err := validateRandomCompanyColumnsAndSetDefault(ctx, columns, engineMode); err != nil {
		return nil, fmt.Errorf("validate columns: %w", err)
	}

	return &RandomCompanyTransformer{
		t:               t,
		columns:         columns,
		affectedColumns: affectedColumns,
		originalData:    make([]byte, 256),
		engine:          engineMode,
		buf:             bytes.NewBuffer(nil),
		nullableMap:     make(map[int]bool, len(columns)),
	}, nil
}

func (nft *RandomCompanyTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *RandomCompanyTransformer) Init(context.Context) error {
	return nil
}

func (nft *RandomCompanyTransformer) Done(context.Context) error {
	return nil
}

func (nft *RandomCompanyTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	// if we are in hash engine mode, we need to clear buffer before filling it with new data
	nft.originalData = nft.originalData[:0]
	for _, c := range nft.columns {
		rawVal, err := r.GetRawColumnValueByIdx(c.columnIdx)
		if err != nil {
			return fmt.Errorf("unable to get raw value by idx %d: %w", c.columnIdx, err)
		}
		nft.nullableMap[c.columnIdx] = rawVal.IsNull

		if nft.engine == deterministicEngineMode {
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
		return fmt.Errorf("generate company: %w", err)
	}

	for _, c := range nft.columns {
		if c.HashOnly {
			// Skip not affected columns, they can be used for hashing only.
			continue
		}
		if nft.nullableMap[c.columnIdx] && c.KeepNull != nil && *c.KeepNull {
			continue
		}
		nft.buf.Reset()
		err = c.tmpl.Execute(nft.buf, nameAttrs)
		if err != nil {
			return fmt.Errorf("execute template for column %s: %w", c.Name, err)
		}
		newRawVal := commonmodels.NewColumnRawValue(slices.Clone(nft.buf.Bytes()), false)
		if err = r.SetRawColumnValueByIdx(c.columnIdx, newRawVal); err != nil {
			return fmt.Errorf("set new value for column \"%s\": %w", c.Name, err)
		}
	}
	return nil
}

func validateRandomCompanyColumnsAndSetDefault(
	ctx context.Context,
	columns []*randomCompanyNameColumns,
	engineMode int,
) error {
	var hasHashingColumns bool

	for idx, c := range columns {
		if c.Name == "" {
			validationcollector.FromContext(ctx).
				Add(commonmodels.NewValidationWarning().
					SetSeverity(commonmodels.ValidationSeverityError).
					AddMeta("ParameterName", "columns").
					AddMeta("ListIdx", idx).
					SetMsg("name is required"))
			return commonmodels.ErrFatalValidationError
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

	if !hasHashingColumns && engineMode == deterministicEngineMode {
		for _, c := range columns {
			log.Ctx(ctx).Debug().
				Str("ColumnName", c.Name).
				Msg("no columns marked for hashing, marking all columns for hashing")
			c.Hashing = true
		}
	}

	return nil
}
