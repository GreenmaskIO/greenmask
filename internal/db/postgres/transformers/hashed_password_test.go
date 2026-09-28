package transformers

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const testPassword = "Password123!"

// hashedPasswordParams builds transformer parameters from name and value pairs.
func hashedPasswordParams(pairs ...string) map[string]toolkit.ParamsValue {
	m := make(map[string]toolkit.ParamsValue, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = toolkit.ParamsValue(pairs[i+1])
	}
	return m
}

// hashedPasswordFor runs the transformer on one record and returns the new column value.
func hashedPasswordFor(t *testing.T, p map[string]toolkit.ParamsValue, original string, resolveEnv bool) (string, bool) {
	t.Helper()
	driver, record := getDriverAndRecord("data", original)
	p["column"] = toolkit.ParamsValue("data")
	transformerCtx, warnings, err := HashedPasswordTransformerDefinition.Instance(
		context.Background(), driver, p, nil, "", resolveEnv,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformerCtx.Transformer.Transform(context.Background(), record)
	require.NoError(t, err)
	val, err := r.GetColumnValueByName("data")
	require.NoError(t, err)
	if val.IsNull {
		return "", true
	}
	encoded, err := r.Encode()
	require.NoError(t, err)
	res, err := encoded.Encode()
	require.NoError(t, err)
	return string(res), false
}

func TestHashedPasswordTransformer_Transform(t *testing.T) {
	tests := []struct {
		name       string
		params     map[string]toolkit.ParamsValue
		original   string
		wantPrefix string
		wantNull   bool
	}{
		{
			name:       "default variant is 2a",
			params:     hashedPasswordParams("password", testPassword, "cost", "4"),
			original:   "old-hash",
			wantPrefix: "$2a$04$",
		},
		{
			name:       "variant 2b",
			params:     hashedPasswordParams("password", testPassword, "cost", "4", "bcrypt_variant", "2b"),
			original:   "old-hash",
			wantPrefix: "$2b$04$",
		},
		{
			name:       "variant 2y",
			params:     hashedPasswordParams("password", testPassword, "cost", "4", "bcrypt_variant", "2y"),
			original:   "old-hash",
			wantPrefix: "$2y$04$",
		},
		{
			name:     "keep_null true keeps NULL",
			params:   hashedPasswordParams("password", testPassword, "cost", "4"),
			original: "\\N",
			wantNull: true,
		},
		{
			name:       "keep_null false hashes NULL",
			params:     hashedPasswordParams("password", testPassword, "cost", "4", "keep_null", "false"),
			original:   "\\N",
			wantPrefix: "$2a$04$",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isNull := hashedPasswordFor(t, tt.params, tt.original, false)
			require.Equal(t, tt.wantNull, isNull)
			if tt.wantNull {
				return
			}
			require.True(t, strings.HasPrefix(got, tt.wantPrefix), "got %q, want prefix %q", got, tt.wantPrefix)
			require.NoError(t, bcrypt.CompareHashAndPassword([]byte(got), []byte(testPassword)))
		})
	}
}

func TestHashedPasswordTransformer_SaltModes(t *testing.T) {
	twoHashes := func(t *testing.T, perRowSalt string) (string, string) {
		driver, first := getDriverAndRecord("data", "a")
		_, second := getDriverAndRecord("data", "b")
		p := hashedPasswordParams("column", "data", "password", testPassword, "cost", "4", "per_row_salt", perRowSalt)
		transformerCtx, warnings, err := HashedPasswordTransformerDefinition.Instance(
			context.Background(), driver, p, nil, "", false,
		)
		require.NoError(t, err)
		require.Empty(t, warnings)

		var hashes []string
		for _, r := range []*toolkit.Record{first, second} {
			r, err := transformerCtx.Transformer.Transform(context.Background(), r)
			require.NoError(t, err)
			val, err := r.GetRawColumnValueByIdx(0)
			require.NoError(t, err)
			require.NoError(t, bcrypt.CompareHashAndPassword(val.Data, []byte(testPassword)))
			hashes = append(hashes, string(val.Data))
		}
		return hashes[0], hashes[1]
	}

	t.Run("shared hash by default", func(t *testing.T) {
		first, second := twoHashes(t, "false")
		require.Equal(t, first, second)
	})
	t.Run("per_row_salt gives each row its own hash", func(t *testing.T) {
		first, second := twoHashes(t, "true")
		require.NotEqual(t, first, second)
	})
}

func TestHashedPasswordTransformer_PasswordFromEnv(t *testing.T) {
	t.Setenv("TEST_LOGIN_PASSWORD", "from-env")
	p := hashedPasswordParams("password", "${TEST_LOGIN_PASSWORD}", "cost", "4")
	got, _ := hashedPasswordFor(t, p, "old-hash", true)
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(got), []byte("from-env")))
}

func TestHashedPasswordTransformer_Validation(t *testing.T) {
	longPassword := strings.Repeat("x", bcryptMaxPasswordBytes+1)
	tests := []struct {
		name    string
		params  map[string]toolkit.ParamsValue
		wantMsg string
	}{
		{name: "no password", params: hashedPasswordParams()},
		{name: "empty password", params: hashedPasswordParams("password", ""), wantMsg: "password is empty"},
		{name: "password too long", params: hashedPasswordParams("password", longPassword), wantMsg: "longer than 72 bytes"},
		{name: "unknown variant", params: hashedPasswordParams("password", testPassword, "bcrypt_variant", "2x")},
		{name: "cost too low", params: hashedPasswordParams("password", testPassword, "cost", "3"), wantMsg: "cost must be from 4 to 31"},
		{name: "cost too high", params: hashedPasswordParams("password", testPassword, "cost", "32"), wantMsg: "cost must be from 4 to 31"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, _ := getDriverAndRecord("data", "old-hash")
			tt.params["column"] = toolkit.ParamsValue("data")
			_, warnings, err := HashedPasswordTransformerDefinition.Instance(
				context.Background(), driver, tt.params, nil, "", false,
			)
			require.NoError(t, err)
			require.NotEmpty(t, warnings)
			require.True(t, warnings.IsFatal())
			if tt.wantMsg != "" {
				require.Contains(t, warnings[0].Msg, tt.wantMsg)
			}
			// the password must never appear in a warning
			for _, w := range warnings {
				require.NotContains(t, w.Msg, longPassword)
				for _, v := range w.Meta {
					require.NotEqual(t, longPassword, v)
					require.NotEqual(t, testPassword, v)
				}
			}
		})
	}
}

func TestSetBcryptVariant(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(hash), "$2a$"))

	got, err := setBcryptVariant(hash, 'y')
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(got), "$2y$"))

	_, err = setBcryptVariant([]byte("not-a-hash"), 'y')
	require.Error(t, err)
}
