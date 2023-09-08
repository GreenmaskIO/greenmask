package transformers

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/scrypt"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

// TODO: Make length truncation

const (
	saltLength = 32
)

var HashTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"Hash",
		"Generate hash of column value",
		toolkit.TupleTransformation,
	),
	NewHashTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("text", "varchar"),
		).SetRequired(true),
	toolkit.MustNewParameter("salt", "salt for hash", new(string), nil),
)

type HashTransformer struct {
	salt       []byte
	columnName string
}

func NewHashTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	p := parameters["column"]
	var columnName string
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	var saltStr string
	var salt []byte
	p = parameters["salt"]
	if err := p.Scan(&saltStr); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	if saltStr == "" {
		b := make([]byte, saltLength)
		if _, err := crand.Read(b); err != nil {
			return nil, nil, err
		}
		salt = b
	} else {
		salt = []byte(saltStr)
	}

	return &HashTransformer{
		salt:       salt,
		columnName: columnName,
	}, nil, nil
}

func (ht *HashTransformer) Init(ctx context.Context) error {
	return nil
}

func (ht *HashTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}

func (ht *HashTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var originalValue string
	if err := r.ScanAttribute(ht.columnName, &originalValue); err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}

	dk, err := scrypt.Key([]byte(originalValue), ht.salt, 32768, 8, 1, 32)
	if err != nil {
		return nil, fmt.Errorf("cannot perform hash calculation: %w", err)
	}

	res := base64.StdEncoding.EncodeToString(dk)
	if err := r.SetAttribute(ht.columnName, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(HashTransformerDefinition)
}
