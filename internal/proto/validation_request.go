package proto

const (
	ValidationRequestCommandId = 'v'
)

type ValidationRequest struct {
	metaData []byte
}

func (ar *ValidationRequest) Decode(src []byte) error {
	return DecodeMessage(src, ar)
}

func (ar *ValidationRequest) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, ar)
}

func (ar *ValidationRequest) CommandId() rune {
	return ValidationRequestCommandId
}

func (ar *ValidationRequest) Backend() {}
