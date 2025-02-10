package transformers

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"text/template"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const emailTransformerGeneratorSize = 64

const RandomEmailTransformerName = "RandomEmail"

var emailTransformerRegexp = regexp.MustCompile(`^([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)$`)

//var emailTransformerAllowedChars = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&'*+-/=?^_`{|}~.")

var EmailTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomEmailTransformerName,
		"Generate random email",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewEmailTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes(
			"text", "varchar", "char", "bpchar",
		),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"keep_original_domain",
		`Keep original domain`,
	).SetDefaultValue(toolkit.ParamsValue("false")),

	toolkit.MustNewParameterDefinition(
		"local_part_template",
		`The template for local part of email. By default it is random characters`,
	),

	toolkit.MustNewParameterDefinition(
		"domain_part_template",
		`The template for domain part of email`,
	),

	toolkit.MustNewParameterDefinition(
		"domains",
		`List of domains to use for random email generation`,
	),

	toolkit.MustNewParameterDefinition(
		"validate",
		`validate generated email if using template`,
	).SetDefaultValue(toolkit.ParamsValue("false")),

	toolkit.MustNewParameterDefinition(
		"max_random_length",
		`max length of randomly generated part of the email`,
	).SetDefaultValue(toolkit.ParamsValue("32")),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type EmailTransformer struct {
	g                        generators.Generator
	columnName               string
	columnIdx                int
	validate                 bool
	affectedColumns          map[int]string
	keepNull                 bool
	keepOriginalDomain       bool
	domains                  []string
	localPartTemplate        *template.Template
	domainTemplate           *template.Template
	templetCtx               map[string]any
	buf                      *bytes.Buffer
	originalDomain           []byte
	hexEncodedRandomBytesBuf []byte
	rctx                     *toolkit.RecordContext
}

func NewEmailTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, engine, localPartTemplate, domainTemplate string
	var keepNull, keepOriginalDomain /*unique,*/, validate bool
	var domains []string
	var err error
	var domainTmpl, localTmpl *template.Template

	var maxLength int

	columnParam := parameters["column"]
	keepOriginalDomainParam := parameters["keep_original_domain"]
	localPartTemplateParam := parameters["local_part_template"]
	domainPartTemplateParam := parameters["domain_part_template"]
	domainsParam := parameters["domains"]
	validateParam := parameters["validate"]
	keepNullParam := parameters["keep_null"]
	engineParam := parameters["engine"]
	maxRamdomLengthParam := parameters["max_random_length"]

	if err = engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if err = columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name \"%s\" is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	if err = keepOriginalDomainParam.Scan(&keepOriginalDomain); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_original_domain" param: %w`, err)
	}

	if err = localPartTemplateParam.Scan(&localPartTemplate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "local_part_template" param: %w`, err)
	}

	if err := domainPartTemplateParam.Scan(&domainTemplate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "domain_part_template" param: %w`, err)
	}

	rrctx := toolkit.NewRecordContext()
	funcMap := toolkit.FuncMap()
	if localPartTemplate != "" || domainTemplate != "" {
		for _, c := range driver.Table.Columns {
			funcMap[c.Name] = func(name string) func() (any, error) {
				return func() (any, error) {
					return rrctx.GetRawColumnValue(name)
				}
			}(c.Name)
		}
	}

	if localPartTemplate != "" {
		localTmpl, err = template.New("local").
			Funcs(funcMap).
			Parse(localPartTemplate)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing \"local_part_template\": %w", err)
		}
	}

	if domainTemplate != "" {
		domainTmpl, err = template.New("domain").
			Funcs(funcMap).
			Parse(domainTemplate)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing \"domain_part_template\": %w", err)
		}
	}

	if err := keepNullParam.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}
	if err := domainsParam.Scan(&domains); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "domains" param: %w`, err)
	}

	if err := validateParam.Scan(&validate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "validate" param: %w`, err)
	}
	if err := maxRamdomLengthParam.Scan(&maxLength); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "max_length" param: %w`, err)
	}

	if maxLength < 1 {
		return nil, nil, errors.New("max_length must be greater than 0")
	}

	g, err := getGenerateEngine(ctx, engine, maxLength)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}

	return &EmailTransformer{
		g:                        g,
		columnName:               columnName,
		keepNull:                 keepNull,
		affectedColumns:          affectedColumns,
		columnIdx:                idx,
		templetCtx:               make(map[string]any, 10),
		keepOriginalDomain:       keepOriginalDomain,
		domains:                  domains,
		localPartTemplate:        localTmpl,
		domainTemplate:           domainTmpl,
		validate:                 validate,
		buf:                      bytes.NewBuffer(nil),
		hexEncodedRandomBytesBuf: make([]byte, hex.EncodedLen(emailTransformerGeneratorSize)),
		rctx:                     rrctx,
	}, nil, nil
}

func (rit *EmailTransformer) GetAffectedColumns() map[int]string {
	return rit.affectedColumns
}

func (rit *EmailTransformer) Init(ctx context.Context) error {
	return nil
}

func (rit *EmailTransformer) Done(ctx context.Context) error {
	return nil
}

func (rit *EmailTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	// TODO: is is null and keepNull is false we can't generate unique value
	//  instead we would use PK or the whole record as input value to hash function
	if val.IsNull && rit.keepNull {
		return r, nil
	}

	defer clear(rit.templetCtx)

	data, err := rit.g.Generate(val.Data)
	if err != nil {
		return nil, fmt.Errorf("unable to generate bytes: %w", err)
	}

	hex.Encode(rit.hexEncodedRandomBytesBuf, data)

	if err := rit.setupTemplateContext(val.Data, r); err != nil {
		return nil, fmt.Errorf("unable to setup template context: %w", err)
	}

	newVal, err := rit.generateEmail(data)
	if err != nil {
		return nil, fmt.Errorf("unable to generate email: %w", err)
	}

	if err = r.SetRawColumnValueByIdx(rit.columnIdx, toolkit.NewRawValue(newVal, false)); err != nil {
		return nil, fmt.Errorf("unable to set new raw value: %w", err)
	}
	return r, nil
}

func (rit *EmailTransformer) setupTemplateContext(originalEmail []byte, r *toolkit.Record) error {
	if rit.localPartTemplate == nil && rit.domainTemplate == nil && !rit.keepOriginalDomain {
		return nil
	}
	rit.rctx.SetRecord(r)

	originalLocalPart, originalDomain, err := EmailParse(originalEmail)
	if err != nil {
		return fmt.Errorf("unable to parse email perfoming keepOriginalDomain operation: %w", err)
	}
	if rit.keepOriginalDomain {
		rit.originalDomain = slices.Clone(originalDomain)
	}

	rit.templetCtx["original_local_part"] = string(originalLocalPart)
	rit.templetCtx["original_domain"] = string(originalDomain)
	rit.templetCtx["random_string"] = string(rit.hexEncodedRandomBytesBuf)

	return nil
}

func (rit *EmailTransformer) generateEmail(data []byte) ([]byte, error) {

	var localPart, domainPart []byte

	if rit.localPartTemplate != nil {
		if err := rit.localPartTemplate.Execute(rit.buf, rit.templetCtx); err != nil {
			return nil, fmt.Errorf("unable to execute local part template: %w", err)
		}
		localPart = slices.Clone(rit.buf.Bytes())
		rit.buf.Reset()
	} else {
		localPart = rit.hexEncodedRandomBytesBuf
	}

	if rit.domainTemplate != nil {
		if err := rit.domainTemplate.Execute(rit.buf, rit.templetCtx); err != nil {
			return nil, fmt.Errorf("unable to execute domain template: %w", err)
		}
		domainPart = slices.Clone(rit.buf.Bytes())
		rit.buf.Reset()
	} else if rit.keepOriginalDomain {
		domainPart = rit.originalDomain
	} else if len(rit.domains) > 0 {
		idx := generators.BuildUint64FromBytes(data[:8]) % uint64(len(rit.domains))
		domainPart = []byte(rit.domains[idx])
	} else {
		idx := generators.BuildUint64FromBytes(data[:8]) % uint64(len(defaultEmailProviders))
		domainPart = []byte(defaultEmailProviders[idx])
	}
	res := append(localPart, '@')
	res = append(res, domainPart...)

	if rit.validate && !EmailValidate(res) {
		log.Debug().
			Str("email", string(res)).
			Msg("generated email is invalid")
		return nil, errors.New("generated email is invalid")
	}
	return res, nil
}

// EmailValidate checks if the email is in a valid format
func EmailValidate(email []byte) bool {
	return emailTransformerRegexp.Match(email)
}

// EmailParse parses the email into local part and domain part, and returns an error if the format is wrong
func EmailParse(email []byte) (localPart, domain []byte, err error) {
	if !EmailValidate(email) {
		return nil, nil, errors.New("invalid email format")
	}
	matches := emailTransformerRegexp.FindSubmatch(email)
	if matches == nil || len(matches) < 3 {
		return nil, nil, errors.New("failed to parse email")
	}
	return matches[1], matches[2], nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(EmailTransformerDefinition)
}
