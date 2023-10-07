package main

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type TestTransformer struct {
	driver     *toolkit.Driver
	parameters map[string]*toolkit.Parameter
}

func NewTestTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (
	toolkit.Transformer, toolkit.ValidationWarnings, error) {

	return &TestTransformer{}, nil, nil
}

func (tt *TestTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}

func (tt *TestTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	return r, nil
}

func main() {

}
