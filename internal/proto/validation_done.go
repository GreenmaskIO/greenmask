package proto

const (
	ValidationDoneCommandId = 'V'
)

type ValidationDone struct {
	metaData []byte
}

func (ar *ValidationDone) Decode(src []byte) error {
	return DecodeMessage(src, ar)
}

func (ar *ValidationDone) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, ar)
}

func (ar *ValidationDone) CommandId() rune {
	return ValidationDoneCommandId
}

func (ar *ValidationDone) Frontend() {}
