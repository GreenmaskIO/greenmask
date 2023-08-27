package base

import "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"

type SchemaValidationFunc func(table *transformers.Table, properties *Properties, parameters []*transformers.Parameter)
