package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func New[T int64 | float64 | string | bool](v T) *T {
	return &v
}

var NoiseIntTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"NoiseInt",
		"Make noise value for int",
		toolkit.TupleTransformation,
	),

	NewNoiseIntTransformer,

	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("int2", "int4", "int8"),
		).SetRequired(true),

	toolkit.MustNewParameter(
		"ratio",
		"max random percentage for noise",
		new(float64),
		New(0.1),
	).SetRequired(true),
)

type NoiseIntTransformer struct {
	columnName string
	ratio      float64
	rand       *rand.Rand
}

func NewNoiseIntTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var ratio float64

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	p = parameters["ratio"]
	if err := p.Scan(&ratio); err != nil {
		return nil, nil, fmt.Errorf("unable to scan type param: %w", err)
	}

	return &NoiseIntTransformer{
		ratio:      ratio,
		columnName: columnName,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil
}

func (nit *NoiseIntTransformer) Init(ctx context.Context) error {
	return nil
}

func (nit *NoiseIntTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	log.Warn().Msg("value out of rage might be possible: double check this transformer implementation")
	if r.IsNull(nit.columnName) {
		return r, nil
	}

	var val int64
	if err := r.ScanAttribute(nit.columnName, &val); err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}

	ratio := nit.rand.Float64() * nit.ratio
	negative := nit.rand.Int63n(2) == 1
	if negative {
		ratio = ratio * -1
	}
	res := val + int64(float64(val)*ratio)
	if err := r.SetAttribute(nit.columnName, &res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseIntTransformerDefinition)
}
