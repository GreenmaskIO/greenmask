package transformers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var NoiseFloatTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"NoiseFloat",
		"Make noise float for int",
		toolkit.TupleTransformation,
	),
	NewNoiseFloatTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("float4", "float8"),
		).SetRequired(true),
	toolkit.MustNewParameter(
		"ratio",
		"max random percentage for noise",
		new(float64),
		New[float64](0.1),
	),
	toolkit.MustNewParameter(
		"precision",
		"precision of noised value",
		new(int64),
		New[int64](4),
	),
)

type NoiseFloatTransformerParams struct {
	Ratio     float64 `mapstructure:"ratio" validate:"required,min=0,max=1"`
	Precision int16   `mapstructure:"precision"`
	Nullable  bool    `mapstructure:"nullable"`
	Fraction  float32 `mapstructure:"fraction"`
}

type NoiseFloatTransformer struct {
	columnName string
	ratio      float64
	precision  float64
	rand       *rand.Rand
}

func NewNoiseFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	log.Warn().Msg("value out of rage might be possible: double check this transformer implementation")
	var columnName string
	var ratio float64
	var precision int64

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	p = parameters["ratio"]
	if err := p.Scan(&ratio); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "ratio" param: %w`, err)
	}

	p = parameters["precision"]
	if err := p.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	return &NoiseFloatTransformer{
		precision:  math.Pow(10, float64(precision)),
		ratio:      ratio,
		columnName: columnName,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil
}

func (nft *NoiseFloatTransformer) Init(ctx context.Context) error {
	return nil
}

func (nft *NoiseFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	valAny, err := r.GetAttribute(nft.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	var val float64
	switch v := valAny.(type) {
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
	if err := r.SetAttribute(nft.columnName, &res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func round(x, unit float64) float64 {
	return math.Floor(x*unit) / unit
}

func init() {
	DefaultTransformerRegistry.MustRegister(NoiseFloatTransformerDefinition)
}
