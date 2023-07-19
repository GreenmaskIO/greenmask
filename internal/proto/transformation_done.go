package proto

const (
	TransformationDoneCommandId = 'S'
)

type TransformationDone struct{}

func (tr *TransformationDone) Decode(src []byte) error {
	return DecodeMessage(src, tr)
}

func (tr *TransformationDone) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, tr)
}

func (tr *TransformationDone) CommandId() rune {
	return TransformationDoneCommandId
}

func (tr *TransformationDone) Backend() {}
