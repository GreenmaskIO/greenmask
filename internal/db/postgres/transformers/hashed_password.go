package transformers

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"golang.org/x/crypto/bcrypt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const HashedPasswordTransformerName = "HashedPassword"

// bcryptMaxPasswordBytes is the longest password bcrypt accepts.
const bcryptMaxPasswordBytes = 72

var HashedPasswordTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		HashedPasswordTransformerName,
		"Replace the value with a bcrypt hash of a known password, so test accounts can log in",
	).AddMeta(AllowApplyForReferenced, false).
		AddMeta(RequireHashEngineParameter, false),

	NewHashedPasswordTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetNullable(true).
		SetAllowedColumnTypes("text", "varchar", "char", "bpchar", "citext"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"password",
		"password to hash. Use resolve_env and ${VAR} to keep it out of the config file",
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"bcrypt_variant",
		"bcrypt version prefix: 2a, 2b or 2y. PostgreSQL pgcrypto accepts only 2a",
	).SetDefaultValue(toolkit.ParamsValue("2a")).
		SetAllowedValues(toolkit.ParamsValue("2a"), toolkit.ParamsValue("2b"), toolkit.ParamsValue("2y")),

	toolkit.MustNewParameterDefinition(
		"cost",
		fmt.Sprintf("bcrypt cost, from %d to %d", bcrypt.MinCost, bcrypt.MaxCost),
	).SetDefaultValue(toolkit.ParamsValue(strconv.Itoa(bcrypt.DefaultCost))).
		SetRawValueValidator(validateBcryptCostParameter),

	toolkit.MustNewParameterDefinition(
		"per_row_salt",
		"hash every row with its own salt. bcrypt is slow on purpose, so by default all rows share one hash",
	).SetDefaultValue(toolkit.ParamsValue("false")),

	toolkit.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

// HashedPasswordTransformer writes a bcrypt hash of one known password into a column,
// so every masked account can log in with that password.
type HashedPasswordTransformer struct {
	columnName      string
	columnIdx       int
	affectedColumns map[int]string
	password        []byte
	variant         byte
	cost            int
	keepNull        bool

	// nextValue returns the value for the next row: one shared hash, or a new
	// salted hash for each row. It is chosen once, in the constructor.
	nextValue func() (*toolkit.RawValue, error)
}

func NewHashedPasswordTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer,
) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, variant string
	var cost int
	var perRowSalt, keepNull bool

	if err := parameters["column"].Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}
	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf(`column with name "%s" is not found`, columnName)
	}

	password, err := parameters["password"].RawValue()
	if err != nil {
		return nil, nil, fmt.Errorf(`unable to read "password" param: %w`, err)
	}
	if warning := validatePassword(password); warning != nil {
		return nil, toolkit.ValidationWarnings{warning}, nil
	}

	if err := parameters["bcrypt_variant"].Scan(&variant); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "bcrypt_variant" param: %w`, err)
	}
	if len(variant) != 2 {
		return nil, nil, fmt.Errorf(`unexpected "bcrypt_variant" value %q`, variant)
	}
	if err := parameters["cost"].Scan(&cost); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "cost" param: %w`, err)
	}
	if err := parameters["per_row_salt"].Scan(&perRowSalt); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "per_row_salt" param: %w`, err)
	}
	if err := parameters["keep_null"].Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	t := &HashedPasswordTransformer{
		columnName:      columnName,
		columnIdx:       idx,
		affectedColumns: map[int]string{idx: columnName},
		password:        bytes.Clone(password),
		variant:         variant[1],
		cost:            cost,
		keepNull:        keepNull,
	}

	if perRowSalt {
		t.nextValue = t.newHash
		return t, nil, nil
	}
	shared, err := t.newHash()
	if err != nil {
		return nil, nil, err
	}
	t.nextValue = func() (*toolkit.RawValue, error) { return shared, nil }
	return t, nil, nil
}

func (t *HashedPasswordTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *HashedPasswordTransformer) Init(ctx context.Context) error {
	return nil
}

func (t *HashedPasswordTransformer) Done(ctx context.Context) error {
	return nil
}

func (t *HashedPasswordTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return r, nil
	}

	newValue, err := t.nextValue()
	if err != nil {
		return nil, err
	}
	if err := r.SetRawColumnValueByIdx(t.columnIdx, newValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

// newHash hashes the password with a new salt and sets the bcrypt version prefix.
func (t *HashedPasswordTransformer) newHash() (*toolkit.RawValue, error) {
	hash, err := bcrypt.GenerateFromPassword(t.password, t.cost)
	if err != nil {
		return nil, fmt.Errorf("hash the password: %w", err)
	}
	hash, err = setBcryptVariant(hash, t.variant)
	if err != nil {
		return nil, err
	}
	return toolkit.NewRawValue(hash, false), nil
}

// setBcryptVariant changes the version prefix of a bcrypt hash, for example $2a$ to $2y$.
// 2a, 2b and 2y are the same algorithm; verifiers differ in which prefixes they accept.
func setBcryptVariant(hash []byte, variant byte) ([]byte, error) {
	if len(hash) < 4 || hash[0] != '$' || hash[1] != '2' || hash[3] != '$' {
		return nil, fmt.Errorf("unexpected bcrypt hash format")
	}
	hash[2] = variant
	return hash, nil
}

// validatePassword never puts the password into the warning.
func validatePassword(password []byte) *toolkit.ValidationWarning {
	switch {
	case len(password) == 0:
		return toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterName", "password").
			SetMsg("password is empty")
	case len(password) > bcryptMaxPasswordBytes:
		return toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterName", "password").
			SetMsg(fmt.Sprintf("password is longer than %d bytes, the bcrypt limit", bcryptMaxPasswordBytes))
	}
	return nil
}

func validateBcryptCostParameter(p *toolkit.ParameterDefinition, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	cost, err := strconv.Atoi(string(v))
	if err != nil {
		return nil, fmt.Errorf(`error parsing "cost" as integer: %w`, err)
	}
	if cost >= bcrypt.MinCost && cost <= bcrypt.MaxCost {
		return nil, nil
	}
	return toolkit.ValidationWarnings{
		toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterValue", string(v)).
			SetMsg(fmt.Sprintf("cost must be from %d to %d", bcrypt.MinCost, bcrypt.MaxCost)),
	}, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(HashedPasswordTransformerDefinition)
}
