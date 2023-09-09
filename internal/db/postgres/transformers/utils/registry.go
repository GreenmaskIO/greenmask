package utils

import (
	"fmt"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var DefaultTransformerRegistry = NewTransformerRegistry()

type TransformerRegistry struct {
	m map[string]*toolkit.Definition
}

func NewTransformerRegistry() *TransformerRegistry {
	return &TransformerRegistry{
		m: make(map[string]*toolkit.Definition),
	}
}

func (tm *TransformerRegistry) Register(definition *toolkit.Definition) error {
	if _, ok := tm.m[definition.Properties.Name]; ok {
		return fmt.Errorf("unable to register transformer: transformer with name %s already exists",
			definition.Properties.Name)
	}
	tm.m[definition.Properties.Name] = definition
	return nil
}

func (tm *TransformerRegistry) MustRegister(definition *toolkit.Definition) {
	if err := tm.Register(definition); err != nil {
		panic(err.Error())
	}
}

func (tm *TransformerRegistry) Get(name string) (*toolkit.Definition, bool) {
	t, ok := tm.m[name]
	return t, ok
}
