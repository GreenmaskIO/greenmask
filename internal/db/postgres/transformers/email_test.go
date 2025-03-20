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
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomEmailTransformer_Transform(t *testing.T) {
	validEmailRegexp := regexp.MustCompile(`^([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)$`)
	def, ok := utils.DefaultTransformerRegistry.Get("RandomEmail")
	require.True(t, ok)

	tests := []struct {
		name        string
		params      map[string]toolkit.ParamsValue
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
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
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
			params: map[string]toolkit.ParamsValue{
				"column":            toolkit.ParamsValue("data"),
				"max_random_length": toolkit.ParamsValue("10"),
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
			params: map[string]toolkit.ParamsValue{
				"column":    toolkit.ParamsValue("data"),
				"keep_null": toolkit.ParamsValue("true"),
			},
			isNull: true,
		},
		{
			name:       "keep_null false and NULL value",
			original:   "\\N",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"column":    toolkit.ParamsValue("data"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
			},
		},
		{
			name:       "keep_original_domain",
			original:   "lucky@luke.be",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"column":               toolkit.ParamsValue("data"),
				"keep_original_domain": toolkit.ParamsValue("true"),
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
			params: map[string]toolkit.ParamsValue{
				"column":  toolkit.ParamsValue("data"),
				"domains": toolkit.ParamsValue(`["haddock.org", "dupont.net"]`),
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
			params: map[string]toolkit.ParamsValue{
				"column":              toolkit.ParamsValue("data"),
				"local_part_template": toolkit.ParamsValue("prefix_{{.random_string | trunc 10}}"),
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
			params: map[string]toolkit.ParamsValue{
				"column":              toolkit.ParamsValue("data"),
				"local_part_template": toolkit.ParamsValue("prefix_{{.random_string}}"),
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
			params: map[string]toolkit.ParamsValue{
				"column":               toolkit.ParamsValue("data"),
				"domain_part_template": toolkit.ParamsValue("custom-domain.com"),
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
			params: map[string]toolkit.ParamsValue{
				"column":              toolkit.ParamsValue("data"),
				"local_part_template": toolkit.ParamsValue("prefix@,&@"),
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
			params: map[string]toolkit.ParamsValue{
				"column":              toolkit.ParamsValue("data"),
				"validate":            toolkit.ParamsValue("true"),
				"local_part_template": toolkit.ParamsValue("prefix@,&@"),
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
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"engine": toolkit.ParamsValue("hash"),
			},
			validateFn: func(t *testing.T, originalEmail, transformedEmail string) {
				assert.True(t, validEmailRegexp.MatchString(transformedEmail))
				assert.NotEqual(t, originalEmail, transformedEmail)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.original)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}

			require.NoError(t, err)
			val, err := r.GetColumnValueByName(tt.columnName)
			require.NoError(t, err)
			require.Equal(t, tt.isNull, val.IsNull)

			if !tt.isNull && tt.validateFn != nil {
				transformedEmail, ok := val.Value.(string)
				require.True(t, ok, "Expected string value for transformed email")
				tt.validateFn(t, tt.original, transformedEmail)
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
