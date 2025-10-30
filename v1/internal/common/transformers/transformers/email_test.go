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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestNewEmailTransformer(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := context.Background()
		column := commonmodels.Column{
			Idx:      1,
			Name:     "id",
			TypeName: "text",
			TypeOID:  2,
		}
		tableDriver := mocks.NewTableDriverMock()
		columnParameter := mocks.NewParametrizerMock()
		validateParameter := mocks.NewParametrizerMock()
		localPartTemplateParameter := mocks.NewParametrizerMock()
		domainPartTemplateParameter := mocks.NewParametrizerMock()
		domainsParameter := mocks.NewParametrizerMock()
		keepOriginalDomainParameter := mocks.NewParametrizerMock()
		maxRandomLengthParameter := mocks.NewParametrizerMock()
		keepNullParameter := mocks.NewParametrizerMock()
		engineParameter := mocks.NewParametrizerMock()

		// Column parameter calls
		columnParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(column.Name, dest))
			}).Return(nil)
		tableDriver.On("GetColumnByName", column.Name).
			Return(&column, nil)

		// engine parameter calls
		engineParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer("random", dest))
			}).Return(nil)

		// keep_original_domain parameter calls
		keepOriginalDomainParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		// keep_null value parameter calls
		keepNullParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		// domains parameter calls
		domainsParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer([]string{"example.com"}, dest))
			}).Return(nil)

		// validate parameter calls
		validateParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		// max_random_length parameter calls
		maxRandomLengthParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(16, dest))
			}).Return(nil)

		// set expectations for getFuncMapWithColumnGetters
		tableDriver.On("Table").Return(&commonmodels.Table{
			Columns: []commonmodels.Column{column},
		})

		// local_part_template parameter calls
		localPartTemplateParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer("{{ 1 }}", dest))
			}).Return(nil)

		// domain_template parameter calls
		domainPartTemplateParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer("{{ 2 }}", dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":               columnParameter,
			"keep_original_domain": keepOriginalDomainParameter,
			"local_part_template":  localPartTemplateParameter,
			"domain_part_template": domainPartTemplateParameter,
			"domains":              domainsParameter,
			"validate":             validateParameter,
			"max_random_length":    maxRandomLengthParameter,
			"keep_null":            keepNullParameter,
			"engine":               engineParameter,
		}

		tr, err := NewEmailTransformer(ctx, tableDriver, parameters)
		require.NoError(t, err)
		assert.NotNil(t, tr)
		assert.False(t, vc.HasWarnings())
		tran := tr.(*EmailTransformer)
		assert.Equal(t, tran.columnName, column.Name)
		assert.Equal(t, tran.columnIdx, column.Idx)
		assert.Equal(t, tran.affectedColumns, map[int]string{1: "id"})
		assert.True(t, tran.validate)
		assert.Equal(t, tran.domains, []string{"example.com"})
		assert.True(t, tran.keepOriginalDomain, 1)
		assert.NotNil(t, tran.localPartTemplate)
		assert.NotNil(t, tran.domainTemplate)
		assert.NotNil(t, tran.rctx)
		assert.NotEmpty(t, tran.hexEncodedRandomBytesBuf)
		assert.NotNil(t, tran.buf)

		tableDriver.AssertExpectations(t)
		columnParameter.AssertExpectations(t)
		validateParameter.AssertExpectations(t)
		domainPartTemplateParameter.AssertExpectations(t)
		domainsParameter.AssertExpectations(t)
		keepOriginalDomainParameter.AssertExpectations(t)
		maxRandomLengthParameter.AssertExpectations(t)
		keepNullParameter.AssertExpectations(t)
		engineParameter.AssertExpectations(t)
	})
}

func TestRandomEmailTransformer_Transform(t *testing.T) {
	validEmailRegexp := regexp.MustCompile(`^([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)$`)

	tests := []struct {
		name             string
		staticParameters map[string]commonmodels.ParamsValue
		dynamicParameter map[string]commonmodels.DynamicParamValue
		columnName       string
		original         string
		isNull           bool
		validateFn       func(t *testing.T, originalEmail, transformedEmail string)
		expectedErr      string
		columns          []commonmodels.Column
	}{
		{
			name:       "common",
			original:   "dupont@mycompany.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column": commonmodels.ParamsValue("data"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "random_local_short",
			original:   "dupond@mycompany.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":            commonmodels.ParamsValue("data"),
				"max_random_length": commonmodels.ParamsValue("10"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				parts := validEmailRegexp.FindStringSubmatch(transformedEmail)
				require.Len(t, parts, 3)
				localPart := parts[1]
				// Beware, the parameter gives the size in bytes of randomness, it's hex encoded, so twice the size
				assert.Equal(t, 20, len(localPart),
					"Local part should be exactly 20 characters (10 random bytes *2 for hex encoding)")
				// Verify no null characters exist in the entire email
				assert.NotContains(t, transformedEmail, "\x00", "Email should not contain null characters")
				assert.NotEqual(t, originalEmail, transformedEmail)
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "keep_null true and NULL value",
			original:   "\\N",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"keep_null": commonmodels.ParamsValue("true"),
			},
			isNull: true,
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "keep_null false and NULL value",
			original:   "\\N",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"keep_null": commonmodels.ParamsValue("false"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "keep_original_domain",
			original:   "lucky@luke.be",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":               commonmodels.ParamsValue("data"),
				"keep_original_domain": commonmodels.ParamsValue("true"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "@luke.be")
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "custom domains",
			original:   "tintin@milousart.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":  commonmodels.ParamsValue("data"),
				"domains": commonmodels.ParamsValue(`["haddock.org", "dupont.net"]`),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.True(t,
					transformedEmail[len(transformedEmail)-11:] == "haddock.org" ||
						transformedEmail[len(transformedEmail)-10:] == "dupont.net")
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			// verify that we can truncate the random string, used to avoid a bug in random buffer handling
			name:       "local_part_template truncated",
			original:   "lanfeust@detroy.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":              commonmodels.ParamsValue("data"),
				"local_part_template": commonmodels.ParamsValue("prefix_{{.random_string | trunc 10}}"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix_")
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			// the random_string used to return a buffer with null characters, this test would fail in that case
			name:       "local_part_template",
			original:   "troll@detroy.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":              commonmodels.ParamsValue("data"),
				"local_part_template": commonmodels.ParamsValue("prefix_{{.random_string}}"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix_")
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "domain_part_template",
			original:   "cixi@detroy.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":               commonmodels.ParamsValue("data"),
				"domain_part_template": commonmodels.ParamsValue("custom-domain.com"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "@custom-domain.com")
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "use template to generate an invalid email with validate false",
			original:   "cian@detroy.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":              commonmodels.ParamsValue("data"),
				"local_part_template": commonmodels.ParamsValue("prefix@,&@"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.False(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix@,&@")
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "use template to generate an invalid email with validate true",
			original:   "nicodeme@detroy.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":              commonmodels.ParamsValue("data"),
				"validate":            commonmodels.ParamsValue("true"),
				"local_part_template": commonmodels.ParamsValue("prefix@,&@"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.False(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix@,&@")
			},
			expectedErr: "generated email is invalid",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  23,
				},
			},
		},
		{
			name:       "common hash",
			original:   "dupont@mycompany.com",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column": commonmodels.ParamsValue("data"),
				"engine": commonmodels.ParamsValue("hash"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: "text",
					TypeOID:  mysqldbmsdriver.VirtualOidText,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				EmailTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, commonmodels.NewColumnRawValue([]byte(tt.original), tt.isNull))

			err = env.Transform(t, ctx)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			rec := env.GetRecord()
			val, err := rec.GetRawColumnValueByName(tt.columnName)
			require.NoError(t, err)
			require.Equal(t, tt.isNull, val.IsNull)
			if !tt.isNull && tt.validateFn != nil {
				tt.validateFn(t, tt.original, string(val.Data))
			}
		})
	}
}

func TestEmailParse(t *testing.T) {
	tests := []struct {
		name          string
		email         string
		wantLocalPart string
		wantDomain    string
		wantErr       bool
	}{
		{
			name:          "valid email",
			email:         "test@example.com",
			wantLocalPart: "test",
			wantDomain:    "example.com",
			wantErr:       false,
		},
		{
			name:          "valid email with complex local part",
			email:         "test.user+tag@example.com",
			wantLocalPart: "test.user+tag",
			wantDomain:    "example.com",
			wantErr:       false,
		},
		{
			name:          "invalid email - no @",
			email:         "testexample.com",
			wantLocalPart: "",
			wantDomain:    "",
			wantErr:       true,
		},
		{
			name:          "invalid email - empty local part",
			email:         "@example.com",
			wantLocalPart: "",
			wantDomain:    "",
			wantErr:       true,
		},
		{
			name:          "invalid email - empty domain",
			email:         "test@",
			wantLocalPart: "",
			wantDomain:    "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localPart, domain, err := EmailParse([]byte(tt.email))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantLocalPart, string(localPart))
			assert.Equal(t, tt.wantDomain, string(domain))
		})
	}
}

func TestEmailValidate(t *testing.T) {
	tests := []struct {
		name  string
		email string
		want  bool
	}{
		{
			name:  "valid simple email",
			email: "test@example.com",
			want:  true,
		},
		{
			name:  "valid email with dot in local part",
			email: "test.user@example.com",
			want:  true,
		},
		{
			name:  "valid email with plus in local part",
			email: "test+tag@example.com",
			want:  true,
		},
		{
			name:  "valid email with subdomain",
			email: "test@sub.example.com",
			want:  true,
		},
		{
			name:  "invalid email - no @",
			email: "testexample.com",
			want:  false,
		},
		{
			name:  "invalid email - empty local part",
			email: "@example.com",
			want:  false,
		},
		{
			name:  "invalid email - empty domain",
			email: "test@",
			want:  false,
		},
		{
			name:  "invalid email - space in local part",
			email: "test user@example.com",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EmailValidate([]byte(tt.email))
			assert.Equal(t, tt.want, got)
		})
	}
}
