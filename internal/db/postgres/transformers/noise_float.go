package transformers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit"
)

var NoiseFloatTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"NoiseFloat",
		"Make noise float for int",
	),
	NewNoiseFloatTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8").
		SetSkipOnNull(true),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"ratio",
		"max random percentage for noise",
	).SetDefaultValue(toolkit.ParamsValue("0.1")),

	toolkit.MustNewParameter(
		"precision",
		"precision of noised value",
	).SetDefaultValue(toolkit.ParamsValue("4")),
)

type NoiseFloatTransformerParams struct {
	Ratio     float64 `mapstructure:"ratio" validate:"required,min=0,max=1"`
	Precision int16   `mapstructure:"precision"`
	Nullable  bool    `mapstructure:"nullable"`
	Fraction  float32 `mapstructure:"fraction"`
}

type NoiseFloatTransformer struct {
	columnName      string
	ratio           float64
	precision       float64
	rand            *rand.Rand
	affectedColumns map[int]string
}

func NewNoiseFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	// TODO: value out of rage might be possible: double check this transformer implementation

	var columnName string
	var ratio float64
	var precision int64

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["ratio"]
	if err := p.Scan(&ratio); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "ratio" param: %w`, err)
	}

	p = parameters["precision"]
	if err := p.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	return &NoiseFloatTransformer{
		precision:       math.Pow(10, float64(precision)),
		ratio:           ratio,
		columnName:      columnName,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
	}, nil, nil
}

func (nft *NoiseFloatTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *NoiseFloatTransformer) Init(ctx context.Context) error {
	return nil
}

func (nft *NoiseFloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (nft *NoiseFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetAttributeByName(nft.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull {
		return r, nil
	}

	var val float64
	switch v := valAny.Value.(type) {
	case float64:
		val = v
	case float32:
		val = float64(v)
	default:
		return nil, errors.New("unknown scanned type")
	}

	ratio := nft.rand.Float64() * nft.ratio
	negative := nft.rand.Int63n(2) == 1
	if negative {
		ratio = ratio * -1
	}
	res := round(val+val*ratio, nft.precision)
	if err := r.SetAttributeByName(nft.columnName, &res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func round(x, unit float64) float64 {
	return math.Floor(x*unit) / unit
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseFloatTransformerDefinition)
}
