package proto

import "fmt"

type CodecError struct {
	Err         error
	Msg         string
	MessageType rune
}

func (ce *CodecError) Error() string {
	if ce.Err != nil {
		return fmt.Sprintf("codec error: %s", ce.Err.Error())
	}
	return "codec error"
}

func (ce *CodecError) Unwrap() error {
	return ce.Err
}
