package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

func New[T int64 | float64 | string | bool](v T) *T {
	return &v
}

var NoiseIntTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"NoiseInt",
		"Make noise value for int",
	),

	NewNoiseIntTransformer,

	toolkit2.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit2.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("int2", "int4", "int8"),
		).SetRequired(true),

	toolkit2.MustNewParameter(
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

func NewNoiseIntTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
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

func (nit *NoiseIntTransformer) Done(ctx context.Context) error {
	return nil
}

func (nit *NoiseIntTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	// TODO: value out of rage might be possible: double check this transformer implementation

	var val int64
	isNull, err := r.ScanAttribute(nit.columnName, &val)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if isNull {
		return r, nil
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
