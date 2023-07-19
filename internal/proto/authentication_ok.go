package proto

const (
	AuthenticationOkCommandId = 'o'
)

type AuthenticationOk struct{}

func (ao *AuthenticationOk) Decode(src []byte) error {
	return DecodeMessage(src, ao)
}

func (ao *AuthenticationOk) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, ao)
}

func (ao *AuthenticationOk) CommandId() rune {
	return AuthenticationOkCommandId
}

func (ao *AuthenticationOk) Backend() {

}
