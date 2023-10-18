package transformers

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
)

var RandomUuidTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomUuid",
		"Generate random uuid",
	),

	NewRandomUuidTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar", "uuid"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
	).SetDefaultValue(toolkit2.ParamsValue("true")),
)

type RandomUuidTransformer struct {
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
}

func NewRandomUuidTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var columnName string
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomUuidTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		affectedColumns: affectedColumns,
	}, nil, nil
}

func (rut *RandomUuidTransformer) GetAffectedColumns() map[int]string {
	return rut.affectedColumns
}

func (rut *RandomUuidTransformer) Init(ctx context.Context) error {
	return nil
}

func (rut *RandomUuidTransformer) Done(ctx context.Context) error {
	return nil
}

func (rut *RandomUuidTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	val, err := r.GetRawAttributeValueByName(rut.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rut.keepNull {
		return r, nil
	}

	if err = r.SetAttributeByName(rut.columnName, uuid.New()); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomUuidTransformerDefinition)
}
