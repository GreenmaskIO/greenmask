package generators

type Transformer interface {
	Transform([]byte) ([]byte, error)
}
