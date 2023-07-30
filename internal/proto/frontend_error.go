package proto

const FrontendErrorCommandId = 'e'

const (
	ValidationFrontendErrorType = "ValidationError"
	RuntimeFrontendErrorType    = "RuntimeError"
)

type FrontendError struct {
	Type string `json:"type,omitempty"`
	Data []byte `json:"data,omitempty"`
}

func (be *FrontendError) Decode(src []byte) error {
	return DecodeMessage(src, be)
}

func (be *FrontendError) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, be)
}

func (be *FrontendError) CommandId() rune {
	return FrontendErrorCommandId
}

func (be *FrontendError) Frontend() {}
