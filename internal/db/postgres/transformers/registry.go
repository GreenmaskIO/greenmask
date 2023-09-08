package transformers

import (
	"fmt"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var DefaultTransformerRegistry = NewTransformerRegistry()

type TransformerRegistry struct {
	M map[string]*toolkit.Definition
}

func NewTransformerRegistry() *TransformerRegistry {
	return &TransformerRegistry{
		M: make(map[string]*toolkit.Definition),
	}
}

func (tm *TransformerRegistry) Register(definition *toolkit.Definition) error {
	if _, ok := tm.M[definition.Properties.Name]; ok {
		return fmt.Errorf("unable to register transformer: transformer with name %s already exists",
			definition.Properties.Name)
	}
	return nil
}

func (tm *TransformerRegistry) MustRegister(definition *toolkit.Definition) {
	if err := tm.Register(definition); err != nil {
		panic(err.Error())
	}
}
