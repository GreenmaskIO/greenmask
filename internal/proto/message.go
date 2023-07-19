package proto

type Message interface {
	Decode(data []byte) error
	Encode(dst []byte) ([]byte, error)
	CommandId() rune
}
