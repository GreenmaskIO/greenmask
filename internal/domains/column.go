package domains

import "github.com/wwoytenko/greenfuscator/internal/masker"

type Column struct {
	Name    string        `yaml:"name"`
	Type    string        `yaml:"type"`
	Masker  masker.Masker `yaml:"masker"`
	Params  []string      `yaml:"params"`
	NotNull bool
}
