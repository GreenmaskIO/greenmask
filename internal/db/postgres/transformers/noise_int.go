package transformers

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/GreenmaskIO/greenmask/pkg/toolkit/transformers"
)

const NoiseIntTransformerName = "NoiseInt"

func New[T int64 | float64](v T) *T {
	return &v
}

var NoiseIntTransformerDefinition = transformers.NewDefinition(
	transformers.MustNewTransformerProperties(
		"NoiseInt",
		"Make noise value for int",
		transformers.TupleTransformation,
	),
	NewNoiseIntTransformer,
	transformers.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(transformers.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("int2", "int4", "int8"),
		).SetRequired(true),
	transformers.MustNewParameter(
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

func NewNoiseIntTransformer(ctx context.Context, driver *transformers.Driver, parameters map[string]*transformers.Parameter) (transformers.Transformer, transformers.ValidationWarnings, error) {
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

func (nit *NoiseIntTransformer) Validate(ctx context.Context) (transformers.ValidationWarnings, error) {
	return nil, nil
}

func (nit *NoiseIntTransformer) Transform(ctx context.Context, r *transformers.Record) (*transformers.Record, error) {
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
