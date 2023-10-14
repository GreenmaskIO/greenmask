package utils

import (
	"fmt"
)

var DefaultTransformerRegistry = NewTransformerRegistry()

type TransformerRegistry struct {
	m map[string]*Definition
}

func NewTransformerRegistry() *TransformerRegistry {
	return &TransformerRegistry{
		m: make(map[string]*Definition),
	}
}

func (tm *TransformerRegistry) Register(definition *Definition) error {
	if _, ok := tm.m[definition.Properties.Name]; ok {
		return fmt.Errorf("unable to register transformer: transformer with Name %s already exists",
			definition.Properties.Name)
	}
	tm.m[definition.Properties.Name] = definition
	return nil
}

func (tm *TransformerRegistry) MustRegister(definition *Definition) {
	if err := tm.Register(definition); err != nil {
		panic(err.Error())
	}
}

func (tm *TransformerRegistry) Get(name string) (*Definition, bool) {
	t, ok := tm.m[name]
	return t, ok
}
