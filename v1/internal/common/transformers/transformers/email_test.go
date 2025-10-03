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

func TestEmailTransformer_Transform(t *testing.T) {
	t.Run("with template", func(t *testing.T) {
		env := newTransformerTestEnv(t, NewEmailTransformer,
			withColumns(commonmodels.Column{
				Idx:      0,
				Name:     "email",
				TypeName: "text",
				TypeOID:  23,
			}),
			withParameter(ParameterNameColumn, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(env.columns["email"].Name, dest))
					}).Return(nil)
			}),
			func(env *transformerTestEnv) {
				// Setup get column call for driver during initialization.
				env.tableDriver.On("GetColumnByName", "email").
					Return(env.getColumnPtr("email"), nil)
			},
			withParameter(ParameterNameEngine, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer("random", dest))
					}).Return(nil)
			}),
			withParameter("keep_original_domain", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(true, dest))
					}).Return(nil)
			}),
			withParameter(ParameterNameKeepNull, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(true, dest))
					}).Return(nil)
			}),
			withParameter("domains", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer([]string{"example.com"}, dest))
					}).Return(nil)
			}),
			withParameter(ParameterNameValidate, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("max_random_length", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(10, dest))
					}).Return(nil)
			}),
			func(env *transformerTestEnv) {
				columns := make([]commonmodels.Column, 0, len(env.columns))
				for _, c := range env.columns {
					columns = append(columns, c)
				}
				env.tableDriver.On("Table").Return(&commonmodels.Table{
					Columns: columns,
				})
			},
			withParameter("local_part_template", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(`{{ "new" }}`, dest))
					}).Return(nil)
			}),
			withParameter("domain_part_template", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(`{{ "new.com" }}`, dest))
					}).Return(nil)
			}),
			withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
				recorder.On("GetRawColumnValueByIdx", env.getColumn("email").Idx).
					Return(commonmodels.NewColumnRawValue([]byte("test@test.com"), false), nil)
				recorder.On("TableDriver").Return(env.tableDriver)
				recorder.On("SetRawColumnValueByIdx",
					env.columns["email"].Idx, commonmodels.NewColumnRawValue([]byte("new@new.com"), false),
				).Return(nil)
			}),
		)

		err := env.transform()
		require.NoError(t, err)
		env.assertExpectations(t)
	})

	t.Run("default", func(t *testing.T) {
		env := newTransformerTestEnv(t, NewEmailTransformer,
			withColumns(commonmodels.Column{
				Idx:      0,
				Name:     "email",
				TypeName: "text",
				TypeOID:  23,
			}),
			withParameter(ParameterNameColumn, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(env.columns["email"].Name, dest))
					}).Return(nil)
			}),
			func(env *transformerTestEnv) {
				// Setup get column call for driver during initialization.
				env.tableDriver.On("GetColumnByName", "email").
					Return(env.getColumnPtr("email"), nil)
			},
			withParameter(ParameterNameEngine, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer("random", dest))
					}).Return(nil)
			}),
			withParameter("keep_original_domain", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter(ParameterNameKeepNull, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("domains", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer([]string{"example.com"}, dest))
					}).Return(nil)
			}),
			withParameter(ParameterNameValidate, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("max_random_length", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(10, dest))
					}).Return(nil)
			}),
			func(env *transformerTestEnv) {
				columns := make([]commonmodels.Column, 0, len(env.columns))
				for _, c := range env.columns {
					columns = append(columns, c)
				}
				env.tableDriver.On("Table").Return(&commonmodels.Table{
					Columns: columns,
				})
			},
			withParameter("local_part_template", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer("", dest))
					}).Return(nil)
			}),
			withParameter("domain_part_template", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer("", dest))
					}).Return(nil)
			}),
			withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
				recorder.On("GetRawColumnValueByIdx", env.getColumn("email").Idx).
					Return(commonmodels.NewColumnRawValue([]byte("test@test.com"), false), nil)
				recorder.On("SetRawColumnValueByIdx",
					env.columns["email"].Idx, mock.MatchedBy(func(v *commonmodels.ColumnRawValue) bool {
						if v.IsNull || len(v.Data) == 0 {
							return false
						}
						local, dom, err := EmailParse(v.Data)
						if err != nil {
							return false
						}
						return len(local) != 10 && string(dom) == "example.com"
					}),
				).Return(nil)
			}),
		)

		err := env.transform()
		require.NoError(t, err)
		env.assertExpectations(t)
	})
}

func TestRandomEmailTransformer_Transform_check_email_parsing(t *testing.T) {
	validEmailRegexp := regexp.MustCompile(`^([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)$`)

	tests := []struct {
		name        string
		params      map[string]any
		columnName  string
		original    string
		isNull      bool
		validateFn  func(t *testing.T, originalEmail, transformedEmail string)
		expectedErr string
	}{
		{
			name:       "common",
			original:   "dupont@mycompany.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"engine":               "random",
				"keep_original_domain": false,
				"keep_null":            false,
				"domains":              nil,
				"validate":             false,
				"max_random_length":    16,
				"local_part_template":  "",
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
			},
		},
		{
			name:       "random_local_short",
			original:   "dupond@mycompany.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"max_random_length":    10,
				"engine":               "random",
				"keep_original_domain": false,
				"keep_null":            false,
				"domains":              nil,
				"validate":             false,
				"local_part_template":  "",
				"domain_part_template": "",
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
		},
		{
			name:       "keep_null true and NULL value",
			original:   "\\N",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"domains":              nil,
				"validate":             false,
				"local_part_template":  "",
				"domain_part_template": "",
			},
			isNull: true,
		},
		{
			name:       "keep_null false and NULL value",
			original:   "\\N",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"keep_null":            false,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"domains":              nil,
				"validate":             false,
				"local_part_template":  "",
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
			},
		},
		{
			name:       "keep_original_domain",
			original:   "lucky@luke.be",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"keep_original_domain": true,
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"domains":              nil,
				"validate":             false,
				"local_part_template":  "",
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "@luke.be")
			},
		},
		{
			name:       "custom domains",
			original:   "tintin@milousart.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"domains":              []string{"haddock.org", "dupont.net"},
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"validate":             false,
				"local_part_template":  "",
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.True(t,
					transformedEmail[len(transformedEmail)-11:] == "haddock.org" ||
						transformedEmail[len(transformedEmail)-10:] == "dupont.net")
			},
		},
		{
			// verify that we can truncate the random string, used to avoid a bug in random buffer handling
			name:       "local_part_template truncated",
			original:   "lanfeust@detroy.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"local_part_template":  "prefix_{{.random_string | trunc 10}}",
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"domains":              nil,
				"validate":             false,
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix_")
			},
		},
		{
			// the random_string used to return a buffer with null characters, this test would fail in that case
			name:       "local_part_template",
			original:   "troll@detroy.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"local_part_template":  "prefix_{{.random_string}}",
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"domains":              nil,
				"validate":             false,
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix_")
			},
		},
		{
			name:       "domain_part_template",
			original:   "cixi@detroy.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"domain_part_template": "custom-domain.com",
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"domains":              nil,
				"validate":             false,
				"local_part_template":  "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "@custom-domain.com")
			},
		},
		{
			name:       "use template to generate an invalid email with validate false",
			original:   "cian@detroy.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"local_part_template":  "prefix@,&@",
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"domains":              nil,
				"validate":             false,
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.False(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix@,&@")
			},
		},
		{
			name:       "use template to generate an invalid email with validate true",
			original:   "nicodeme@detroy.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"validate":             true,
				"local_part_template":  "prefix@,&@",
				"keep_null":            true,
				"max_random_length":    16,
				"engine":               "random",
				"keep_original_domain": false,
				"domains":              nil,
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.False(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
				assert.Contains(t, transformedEmail, "prefix@,&@")
			},
			expectedErr: "generated email is invalid",
		},
		{
			name:       "common hash",
			original:   "dupont@mycompany.com",
			columnName: "data",
			params: map[string]any{
				"column":               "data",
				"engine":               "hash",
				"keep_null":            true,
				"max_random_length":    16,
				"keep_original_domain": false,
				"domains":              nil,
				"validate":             false,
				"local_part_template":  "",
				"domain_part_template": "",
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTransformerTestEnv(t, NewEmailTransformer,
				withColumns(commonmodels.Column{
					Idx:      0,
					Name:     tt.columnName,
					TypeName: "text",
					TypeOID:  23,
				}),
				withParametersScanner(tt.params),
				func(env *transformerTestEnv) {
					// Setup get column call for driver during initialization.
					env.tableDriver.On("GetColumnByName", tt.columnName).
						Return(env.getColumnPtr(tt.columnName), nil)
				},
				func(env *transformerTestEnv) {
					columns := make([]commonmodels.Column, 0, len(env.columns))
					for _, c := range env.columns {
						columns = append(columns, c)
					}
					env.tableDriver.On("Table").Return(&commonmodels.Table{
						Columns: columns,
					})
				},
				func(env *transformerTestEnv) {
					// Setup get column call for driver during initialization.
					env.tableDriver.On("GetColumnByName", tt.columnName).
						Return(env.getColumnPtr(tt.columnName), nil)
				},
				func(env *transformerTestEnv) {
					columns := make([]commonmodels.Column, 0, len(env.columns))
					for _, c := range env.columns {
						columns = append(columns, c)
					}
					env.tableDriver.On("Table").Return(&commonmodels.Table{
						Columns: columns,
					})
				},
				withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
					val := commonmodels.NewColumnRawValue(nil, true)
					if !tt.isNull {
						val = commonmodels.NewColumnRawValue([]byte(tt.original), false)
					}
					recorder.On("GetRawColumnValueByIdx", env.getColumn(tt.columnName).Idx).
						Return(val, nil)
					recorder.On("TableDriver").Return(env.tableDriver)
					recorder.On("SetRawColumnValueByIdx", env.columns[tt.columnName].Idx, mock.Anything).
						Run(func(args mock.Arguments) {
							val := args.Get(1).(*commonmodels.ColumnRawValue)
							require.Equal(t, tt.isNull, val.IsNull)
							if !tt.isNull && tt.validateFn != nil {
								tt.validateFn(t, tt.original, string(val.Data))
							}

						}).Return(nil)
				}),
			)

			err := env.transformer.Transform(context.Background(), env.recorder)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
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
