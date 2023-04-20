package transformers

import (
	"errors"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

func ReplaceTransformer(column domains.Column, val string, params map[string]string) (string, error) {
	val, ok := params["value"]
	if !ok {
		return "", errors.New("expected value key")
	}
	return val, nil
}
