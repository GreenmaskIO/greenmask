package proto

const BackendErrorCommandId = 'E'

const (
	AuthenticationBackendErrorType = "AuthenticationError"
)

type BackendError struct {
	Type string `json:"type,omitempty"`
	Data []byte `json:"data,omitempty"`
}

func (be *BackendError) Decode(src []byte) error {
	return DecodeMessage(src, be)
}

func (be *BackendError) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, be)
}

func (be *BackendError) CommandId() rune {
	return BackendErrorCommandId
}

func (be *BackendError) Backend() {}
