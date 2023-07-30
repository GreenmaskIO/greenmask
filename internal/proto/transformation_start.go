package proto

const (
	TransformationStartCommandId = 's'
)

type TransformationStart struct{}

func (tr *TransformationStart) Decode(src []byte) error {
	return DecodeMessage(src, tr)
}

func (tr *TransformationStart) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, tr)
}

func (tr *TransformationStart) CommandId() rune {
	return TransformationStartCommandId
}

func (tr *TransformationStart) Backend() {}
