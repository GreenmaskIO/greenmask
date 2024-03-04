package transformers

import "context"

type Transformer interface {
	Transform(ctx context.Context, data []byte) (res []byte, err error)
}
