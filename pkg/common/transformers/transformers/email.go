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
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"text/template"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	generators2 "github.com/greenmaskio/greenmask/pkg/common/transformers/generators"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	template3 "github.com/greenmaskio/greenmask/pkg/common/transformers/template"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
)

const TransformerNameRandomEmail = "RandomEmail"

var (
	emailTransformerRegexp = regexp.MustCompile(`^([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)$`)

	errGeneratedEmailIsInvalid = errors.New("generated email is invalid")
)

// Predefined global variable containing a list of top email providers as a slice of strings
var defaultEmailProviders = []string{
	"gmail.com",      // Google Gmail
	"yahoo.com",      // Yahoo Mail
	"outlook.com",    // Microsoft Outlook
	"hotmail.com",    // Microsoft Hotmail (now part of Outlook)
	"aol.com",        // AOL Mail
	"icloud.com",     // Apple iCloud Mail
	"mail.com",       // Mail.com
	"zoho.com",       // Zoho Mail
	"yandex.com",     // Yandex Mail
	"protonmail.com", // ProtonMail
	"gmx.com",        // GMX Mail
	"fastmail.com",   // Fastmail
}

//var emailTransformerAllowedChars = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&'*+-/=?^_`{|}~.")

var EmailTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameRandomEmail,
		"Generate random email",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewEmailTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(models.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(models.TypeClassText),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"keep_original_domain",
		`Keep original domain`,
	).SetDefaultValue(models.ParamsValue("false")),

	parameters.MustNewParameterDefinition(
		"local_part_template",
		`The template for local part of email. By default it is random characters`,
	),

	parameters.MustNewParameterDefinition(
		"domain_part_template",
		`The template for domain part of email`,
	),

	parameters.MustNewParameterDefinition(
		"domains",
		`List of domains to use for random email generation`,
	),

	parameters.MustNewParameterDefinition(
		"validate",
		`validate generated email if using template`,
	).SetDefaultValue(models.ParamsValue("false")),

	parameters.MustNewParameterDefinition(
		"max_random_length",
		`max length of randomly generated part of the email`,
	).SetDefaultValue(models.ParamsValue("32")).
		SetRawValueValidator(
			func(ctx context.Context, p *parameters.ParameterDefinition, v models.ParamsValue) error {
				// Validate that the value is a positive integer
				intVal, err := strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					validationcollector.FromContext(ctx).Add(
						models.NewValidationWarning().
							SetMsg("error parsing max_random_length").
							AddMeta("ParameterValue", string(v)).
							SetError(err).
							SetSeverity(models.ValidationSeverityError),
					)
					return models.ErrFatalValidationError
				}

				if intVal <= 0 {
					validationcollector.FromContext(ctx).Add(
						models.NewValidationWarning().
							SetMsg("max_random_length must be greater than 0").
							AddMeta("ParameterValue", string(v)).
							SetSeverity(models.ValidationSeverityError),
					)
					return models.ErrFatalValidationError
				}
				return nil
			},
		),

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type EmailTransformer struct {
	g                        generators2.Generator
	columnName               string
	columnIdx                int
	validate                 bool
	affectedColumns          map[int]string
	keepNull                 bool
	keepOriginalDomain       bool
	domains                  []string
	localPartTemplate        *template.Template
	domainTemplate           *template.Template
	templateCtx              map[string]any
	buf                      *bytes.Buffer
	originalDomain           []byte
	hexEncodedRandomBytesBuf []byte
	rctx                     *template3.RecordContextReadOnly
}

// getFuncMapWithColumnGetters - returns a FuncMap with functions to get column values by name.
// The functions are closures that capture the column name and return a function that retrieves
// the raw column value from the RecordContextReadOnly.
func getFuncMapWithColumnGetters(
	tableDriver interfaces.TableDriver,
	rrctx *template3.RecordContextReadOnly,
) template.FuncMap {
	funcMap := template3.FuncMap()
	for _, c := range tableDriver.Table().Columns {
		funcMap[c.Name] = func(name string) func() (any, error) {
			return func() (any, error) {
				return rrctx.GetRawColumnValue(name)
			}
		}(c.Name)
	}
	return funcMap
}

func NewEmailTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
	var domainTmpl, localTmpl *template.Template

	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, err
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, err
	}

	keepOriginalDomain, err := getParameterValueWithName[bool](ctx, parameters, "keep_original_domain")
	if err != nil {
		return nil, err
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, err
	}

	domains, err := getParameterValueWithName[[]string](ctx, parameters, "domains")
	if err != nil {
		return nil, err
	}

	validate, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameValidate)
	if err != nil {
		return nil, err
	}

	maxRandomLength, err := getParameterValueWithName[int](ctx, parameters, "max_random_length")
	if err != nil {
		return nil, err
	}

	rrctx := template3.NewRecordContextReadOnly()
	funcMap := getFuncMapWithColumnGetters(tableDriver, rrctx)

	localPartTemplate, err := getParameterValueWithName[string](ctx, parameters, "local_part_template")
	if err != nil {
		return nil, err
	}
	if localPartTemplate != "" {
		localTmpl, err = template.New("local").
			Funcs(funcMap).
			Parse(localPartTemplate)
		if err != nil {
			return nil, fmt.Errorf("error parsing \"local_part_template\": %w", err)
		}
	}

	domainTemplate, err := getParameterValueWithName[string](ctx, parameters, "domain_part_template")
	if err != nil {
		return nil, err
	}

	if domainTemplate != "" {
		domainTmpl, err = template.New("domain").
			Funcs(funcMap).
			Parse(domainTemplate)
		if err != nil {
			return nil, fmt.Errorf("error parsing \"domain_part_template\": %w", err)
		}
	}

	g, err := getGenerateEngine(ctx, engine, maxRandomLength)
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}
	return &EmailTransformer{
		g:          g,
		columnName: columnName,
		keepNull:   keepNull,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx:                column.Idx,
		templateCtx:              make(map[string]any, 10),
		keepOriginalDomain:       keepOriginalDomain,
		domains:                  domains,
		localPartTemplate:        localTmpl,
		domainTemplate:           domainTmpl,
		validate:                 validate,
		buf:                      bytes.NewBuffer(nil),
		hexEncodedRandomBytesBuf: make([]byte, hex.EncodedLen(maxRandomLength)),
		rctx:                     rrctx,
	}, nil
}

func (t *EmailTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *EmailTransformer) Init(context.Context) error {
	return nil
}

func (t *EmailTransformer) Done(context.Context) error {
	return nil
}

func (t *EmailTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan value: %w", err)
	}
	// TODO: is is null and keepNull is false we can't generate unique value
	//  instead we would use PK or the whole record as input value to hash function
	if val.IsNull && t.keepNull {
		return nil
	}

	defer clear(t.templateCtx)

	data, err := t.g.Generate(val.Data)
	if err != nil {
		return fmt.Errorf("unable to generate bytes: %w", err)
	}

	hex.Encode(t.hexEncodedRandomBytesBuf, data)

	if err := t.setupTemplateContext(val.Data, r); err != nil {
		return fmt.Errorf("unable to setup template context: %w", err)
	}

	newVal, err := t.generateEmail(data)
	if err != nil {
		return fmt.Errorf("unable to generate email: %w", err)
	}

	if err = r.SetRawColumnValueByIdx(t.columnIdx, models.NewColumnRawValue(newVal, false)); err != nil {
		return fmt.Errorf("unable to set new raw value: %w", err)
	}
	return nil
}

func (t *EmailTransformer) Describe() string {
	return TransformerNameRandomEmail
}

func (t *EmailTransformer) setupTemplateContext(originalEmail []byte, r interfaces.Recorder) error {
	if t.localPartTemplate == nil && t.domainTemplate == nil && !t.keepOriginalDomain {
		return nil
	}
	t.rctx.SetRecord(r)

	originalLocalPart, originalDomain, err := EmailParse(originalEmail)
	if err != nil {
		return fmt.Errorf("unable to parse email perfoming keepOriginalDomain operation: %w", err)
	}
	if t.keepOriginalDomain {
		t.originalDomain = slices.Clone(originalDomain)
	}

	t.templateCtx["original_local_part"] = string(originalLocalPart)
	t.templateCtx["original_domain"] = string(originalDomain)
	t.templateCtx["random_string"] = string(t.hexEncodedRandomBytesBuf)

	return nil
}

func (t *EmailTransformer) generateEmail(data []byte) ([]byte, error) {
	var localPart, domainPart []byte

	if t.localPartTemplate != nil {
		if err := t.localPartTemplate.Execute(t.buf, t.templateCtx); err != nil {
			return nil, fmt.Errorf("unable to execute local part template: %w", err)
		}
		localPart = slices.Clone(t.buf.Bytes())
		t.buf.Reset()
	} else {
		localPart = t.hexEncodedRandomBytesBuf
	}

	if t.domainTemplate != nil {
		if err := t.domainTemplate.Execute(t.buf, t.templateCtx); err != nil {
			return nil, fmt.Errorf("unable to execute domain template: %w", err)
		}
		domainPart = slices.Clone(t.buf.Bytes())
		t.buf.Reset()
	} else if t.keepOriginalDomain {
		domainPart = t.originalDomain
	} else if len(t.domains) > 0 {
		idx := generators2.BuildUint64FromBytes(data[:8]) % uint64(len(t.domains))
		domainPart = []byte(t.domains[idx])
	} else {
		idx := generators2.BuildUint64FromBytes(data[:8]) % uint64(len(defaultEmailProviders))
		domainPart = []byte(defaultEmailProviders[idx])
	}
	res := append(localPart, '@')
	res = append(res, domainPart...)

	if t.validate && !EmailValidate(res) {
		log.Debug().
			Str("email", string(res)).
			Msg("generated email is invalid")
		return nil, errGeneratedEmailIsInvalid
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
	if len(matches) == 0 || len(matches) < 3 {
		return nil, nil, errors.New("failed to parse email")
	}
	return matches[1], matches[2], nil
}
