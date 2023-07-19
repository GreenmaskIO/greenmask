package proto

import "github.com/jackc/pgio"

const (
	TransformedPayloadCommandId = 'P'
)

type TransformedPayload struct {
	Payload []byte
}

func (tr *TransformedPayload) Decode(src []byte) error {
	tr.Payload = src
	return nil
}

func (tr *TransformedPayload) Encode(dst []byte) ([]byte, error) {
	dst = append(dst, byte(tr.CommandId()))
	dst = pgio.AppendInt32(dst, int32(4+len(tr.Payload)+1))
	dst = append(dst, tr.Payload...)
	return dst, nil
}

func (tr *TransformedPayload) CommandId() rune {
	return TransformedPayloadCommandId
}

func (tr *TransformedPayload) Frontend() {}
