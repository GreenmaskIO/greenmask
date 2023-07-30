package proto

const (
	AuthenticationRequestCommandId = 'C'
)

const (
	PostgresDbEngine = 'p'
	MySqlDbEngine    = 'm'
	MongoDbEngine    = 'g'
)

type AuthenticationRequest struct {
	Version   int8    `json:"version,omitempty"`
	SessionId [16]int `json:"sessionId,omitempty"`
	JobId     int16   `json:"jobId,omitempty"`
	Engine    string  `json:"engine,omitempty"`
}

func (ar *AuthenticationRequest) Decode(src []byte) error {
	return DecodeMessage(src, ar)
}

func (ar *AuthenticationRequest) Encode(dst []byte) ([]byte, error) {
	return EncodeMessage(dst, ar)
}

func (ar *AuthenticationRequest) CommandId() rune {
	return AuthenticationRequestCommandId
}

func (ar *AuthenticationRequest) Frontend() {}
