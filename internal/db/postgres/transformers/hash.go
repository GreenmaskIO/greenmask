package transformers

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit"
	"golang.org/x/crypto/scrypt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
)

// TODO: Make length truncation

const (
	saltLength = 32
)

var HashTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"Hash",
		"Generate hash of column value",
	),

	NewHashTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"salt",
		"salt for hash",
	),
)

type HashTransformer struct {
	salt            toolkit.ParamsValue
	columnName      string
	affectedColumns map[int]string
}

func NewHashTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
) (utils.Transformer, toolkit.ValidationWarnings, error) {
	p := parameters["column"]
	var columnName string
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	var saltStr string
	var salt toolkit.ParamsValue
	p = parameters["salt"]
	if err := p.Scan(&saltStr); err != nil {
		return nil, nil, fmt.Errorf("unable to parse column param: %w", err)
	}

	if saltStr == "" {
		b := make(toolkit.ParamsValue, saltLength)
		if _, err := crand.Read(b); err != nil {
			return nil, nil, err
		}
		salt = b
	} else {
		salt = toolkit.ParamsValue(saltStr)
	}

	return &HashTransformer{
		salt:            salt,
		columnName:      columnName,
		affectedColumns: affectedColumns,
	}, nil, nil
}

func (ht *HashTransformer) GetAffectedColumns() map[int]string {
	return ht.affectedColumns
}

func (ht *HashTransformer) Init(ctx context.Context) error {
	return nil
}

func (ht *HashTransformer) Done(ctx context.Context) error {
	return nil
}

func (ht *HashTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var originalValue string
	isNull, err := r.ScanAttributeByName(ht.columnName, &originalValue)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if isNull {
		return r, nil
	}

	dk, err := scrypt.Key(toolkit.ParamsValue(originalValue), ht.salt, 32768, 8, 1, 32)
	if err != nil {
		return nil, fmt.Errorf("cannot perform hash calculation: %w", err)
	}

	res := base64.StdEncoding.EncodeToString(dk)
	if err := r.SetAttributeByName(ht.columnName, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(HashTransformerDefinition)
}
