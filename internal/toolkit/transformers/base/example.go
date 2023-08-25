package base

import "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"

var TestTransformerDefinition = &Definition{
	Properties: NewProperties("test", "simple description"),
	New:        NewTestTransformer,
}

type TestTransformer struct {
}

func NewTestTransformer(driver *transformers.Driver) (Transformer, error) {
	return &Transformer{}, nil
}
