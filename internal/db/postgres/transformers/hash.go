package transformers

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"

	"golang.org/x/crypto/scrypt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
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

	toolkit.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"salt",
		"salt for hash",
		new(string),
		nil,
	),
)

type HashTransformer struct {
	salt       domains.ParamsValue
	columnName string
}

func NewHashTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	p := parameters["column"]
	var columnName string
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	var saltStr string
	var salt domains.ParamsValue
	p = parameters["salt"]
	if err := p.Scan(&saltStr); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	if saltStr == "" {
		b := make(domains.ParamsValue, saltLength)
		if _, err := crand.Read(b); err != nil {
			return nil, nil, err
		}
		salt = b
	} else {
		salt = domains.ParamsValue(saltStr)
	}

	return &HashTransformer{
		salt:       salt,
		columnName: columnName,
	}, nil, nil
}

func (ht *HashTransformer) Init(ctx context.Context) error {
	return nil
}

func (ht *HashTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var originalValue string
	isNull, err := r.ScanAttribute(ht.columnName, &originalValue)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if isNull {
		return r, nil
	}

	dk, err := scrypt.Key(domains.ParamsValue(originalValue), ht.salt, 32768, 8, 1, 32)
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
	utils.DefaultTransformerRegistry.MustRegister(HashTransformerDefinition)
}
