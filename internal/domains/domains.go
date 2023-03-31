package domains

import "github.com/wwoytenko/greenfuscator/internal/masker"

type Column struct {
	Name   string        `yaml:"name"`
	Type   string        `yaml:"type"`
	Masker masker.Masker `yaml:"masker"`
	Params []string      `yaml:"params"`
}

type Table struct {
	Schema  string            `yaml:"schema"`
	Name    string            `yaml:"name"`
	Columns map[string]Column `yaml:"columns"`
}

type Tuple struct {
	Table         Table
	OriginalTuple []byte
	MaskedTuple   []byte
}
