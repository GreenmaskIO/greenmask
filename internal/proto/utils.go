package proto

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/jackc/pgio"
)

func DecodeMessage(src []byte, v Message) error {
	i := bytes.IndexByte(src, 0)
	if i != len(src)-1 {
		return &CodecError{
			Msg:         "message format error",
			MessageType: v.CommandId(),
		}
	}

	if err := json.Unmarshal(src, v); err != nil {
		return &CodecError{
			Msg:         "marshalling error",
			MessageType: v.CommandId(),
			Err:         err,
		}
	}

	return nil
}

func EncodeMessage(dst []byte, v Message) ([]byte, error) {
	dst = append(dst, byte(v.CommandId()))
	res, err := json.Marshal(v)
	if err != nil {
		return nil,
			&CodecError{
				Msg:         "unmarshalling error",
				MessageType: v.CommandId(),
				Err:         err,
			}
	}
	dst = pgio.AppendInt32(dst, int32(4+len(res)+1))

	dst = append(dst, res...)
	dst = append(dst, 0)

	return dst, nil
}

func translateEOFtoErrUnexpectedEOF(err error) error {
	if err == io.EOF {
		return &CodecError{
			Msg: "unexpected eof",
			Err: io.ErrUnexpectedEOF,
		}
	}
	return err
}
