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
	"context"
	"fmt"

	"github.com/go-faker/faker/v4"
	"github.com/go-faker/faker/v4/pkg/options"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	RandomLatitudeTransformerName            = "RandomLatitude"
	RandomLongitudeTransformerName           = "RandomLongitude"
	RandomMonthNameTransformerName           = "RandomMonthName"
	RandomYearStringTransformerName          = "RandomYearString"
	RandomDayOfWeekTransformerName           = "RandomDayOfWeek"
	RandomDayOfMonthTransformerName          = "RandomDayOfMonth"
	RandomCenturyTransformerName             = "RandomCentury"
	RandomTimezoneTransformerName            = "RandomTimezone"
	RandomDomainNameTransformerName          = "RandomDomainName"
	RandomURLTransformerName                 = "RandomURL"
	RandomUsernameTransformerName            = "RandomUsername"
	RandomPasswordTransformerName            = "RandomPassword"
	RandomWordTransformerName                = "RandomWord"
	RandomSentenceTransformerName            = "RandomSentence"
	RandomParagraphTransformerName           = "RandomParagraph"
	RandomCCTypeTransformerName              = "RandomCCType"
	RandomCCNumberTransformerName            = "RandomCCNumber"
	RandomCurrencyTransformerName            = "RandomCurrency"
	RandomAmountWithCurrencyTransformerName  = "RandomAmountWithCurrency"
	RandomPhoneNumberTransformerName         = "RandomPhoneNumber"
	RandomTollFreePhoneNumberTransformerName = "RandomTollFreePhoneNumber"
	RandomE164PhoneNumberTransformerName     = "RandomE164PhoneNumber"
)

type FakerFunc func(opts ...options.OptionFunc) string

type FakerTransformerDef struct {
	SupportedTypes []string
	Description    string
	Generator      FakerFunc
}

var FakerTransformersDes = map[string]*FakerTransformerDef{
	// Faker geo
	RandomLatitudeTransformerName: {
		Generator: func(opts ...options.OptionFunc) string {
			return fmt.Sprintf("%f", faker.Latitude())
		},
		SupportedTypes: []string{"float4", "float8", "numeric"},
		Description:    "Generates a random latitude value.",
	},
	RandomLongitudeTransformerName: {
		Generator: func(opts ...options.OptionFunc) string {
			return fmt.Sprintf("%f", faker.Longitude())
		},
		SupportedTypes: []string{"float4", "float8", "numeric"},
		Description:    "Generates a random longitude value.",
	},

	RandomMonthNameTransformerName: {
		Generator:      faker.MonthName,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates the name of a random month.",
	},
	RandomYearStringTransformerName: {
		Generator:      faker.YearString,
		SupportedTypes: []string{"text", "varchar", "int2", "int4", "int8", "numeric"},
		Description:    "Generates a random year as a string.",
	},
	RandomDayOfWeekTransformerName: {
		Generator:      faker.DayOfWeek,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random day of the week.",
	},
	RandomDayOfMonthTransformerName: {
		Generator:      faker.DayOfMonth,
		SupportedTypes: []string{"text", "varchar", "int2", "int4", "int8", "numeric"},
		Description:    "Generates a random day of the month.",
	},
	RandomCenturyTransformerName: {
		Generator:      faker.Century,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random century.",
	},
	RandomTimezoneTransformerName: {
		Generator:      faker.Timezone,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random timezone.",
	},

	// Faker Internet
	RandomDomainNameTransformerName: {
		Generator:      faker.DomainName,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random domain name.",
	},
	RandomURLTransformerName: {
		Generator:      faker.URL,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random URL.",
	},
	RandomUsernameTransformerName: {
		Generator:      faker.Username,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random username.",
	},
	RandomPasswordTransformerName: {
		Generator:      faker.Password,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random password.",
	},

	// Faker words and Sentences
	RandomWordTransformerName: {
		Generator:      faker.Word,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random word.",
	},
	RandomSentenceTransformerName: {
		Generator:      faker.Sentence,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random sentence.",
	},
	RandomParagraphTransformerName: {
		Generator:      faker.Paragraph,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random paragraph.",
	},

	// Faker Payment
	RandomCCTypeTransformerName: {
		Generator:      faker.CCType,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random credit card type.",
	},
	RandomCCNumberTransformerName: {
		Generator:      faker.CCNumber,
		SupportedTypes: []string{"text", "varchar", "int4", "int8", "numeric"},
		Description:    "Generates a random credit card number.",
	},
	RandomCurrencyTransformerName: {
		Generator:      faker.Currency,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random currency code.",
	},
	RandomAmountWithCurrencyTransformerName: {
		Generator:      faker.AmountWithCurrency,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random monetary amount with currency.",
	},

	// Faker Phone
	RandomPhoneNumberTransformerName: {
		Generator:      faker.Phonenumber,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random phone number.",
	},
	RandomTollFreePhoneNumberTransformerName: {
		Generator:      faker.TollFreePhoneNumber,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random toll-free phone number.",
	},
	RandomE164PhoneNumberTransformerName: {
		Generator:      faker.E164PhoneNumber,
		SupportedTypes: []string{"text", "varchar"},
		Description:    "Generates a random phone number in E.164 format.",
	},
}

func generateFakerTransformers(registry *utils.TransformerRegistry) {

	for name, def := range FakerTransformersDes {

		td := utils.NewTransformerDefinition(
			utils.NewTransformerProperties(
				name,
				def.Description,
			),
			MakeNewFakeTransformerFunction(def.Generator),
			toolkit.MustNewParameterDefinition(
				"column",
				"column name",
			).SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetAllowedColumnTypes(def.SupportedTypes...),
			).SetRequired(true),
			toolkit.MustNewParameterDefinition(
				"keep_null",
				"indicates that NULL values must not be replaced with transformed values",
			).SetDefaultValue(
				toolkit.ParamsValue("true"),
			),
		)

		utils.DefaultTransformerRegistry.MustRegister(td)
	}

}

type FakeTransformer struct {
	columnName      string
	keepNull        bool
	columnIdx       int
	affectedColumns map[int]string
	generate        FakerFunc
}

func MakeNewFakeTransformerFunction(generator FakerFunc) utils.NewTransformerFunc {
	return func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
		return NewFakeTransformer(ctx, driver, parameters, generator)
	}
}

func NewFakeTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer, generator FakerFunc,
) (utils.Transformer, toolkit.ValidationWarnings, error) {
	p := parameters["column"]
	var columnName string
	var keepNull bool
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	return &FakeTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		columnIdx:       idx,
		affectedColumns: affectedColumns,
		generate:        generator,
	}, nil, nil
}

func (fts *FakeTransformer) GetAffectedColumns() map[int]string {
	return fts.affectedColumns
}

func (fts *FakeTransformer) Init(ctx context.Context) error {
	return nil
}

func (fts *FakeTransformer) Done(ctx context.Context) error {
	return nil
}

func (fts *FakeTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetRawColumnValueByIdx(fts.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && fts.keepNull {
		return r, nil
	}

	newValue := toolkit.NewRawValue([]byte(fts.generate()), false)

	if err := r.SetRawColumnValueByIdx(fts.columnIdx, newValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil

}

func init() {
	generateFakerTransformers(utils.DefaultTransformerRegistry)
}
