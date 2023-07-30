package custom

import (
	"context"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type PipeExecTransformer struct {
	Meta   *CustomTransformerMeta
	params map[string]interface{}
}

func NewPipeExecTransformer(meta *CustomTransformerMeta, params map[string]interface{}) domains.Transformer {
	return &PipeExecTransformer{
		Meta:   meta,
		params: params,
	}
}

func (pet *PipeExecTransformer) run(ctx context.Context) error {
}

func (pet *PipeExecTransformer) init(args ...string) error {
	// Call init
	// Run goroutine that waits
}

func (pet *PipeExecTransformer) Init(ctx context.Context) error {
	return pet.init()
}

func (pet *PipeExecTransformer) Transform(originalValue string) (string, error) {

}

func (pet *PipeExecTransformer) Validate() domains.RuntimeErrors {

}

func (pet *PipeExecTransformer) IsCustom() bool {
	return true
}
