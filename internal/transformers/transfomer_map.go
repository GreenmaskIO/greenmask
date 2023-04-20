package transformers

import "github.com/wwoytenko/greenfuscator/internal/domains"

var (
	TransformerMap = map[string]domains.TransformerFunc{
		"replace": ReplaceTransformer,
	}
)
