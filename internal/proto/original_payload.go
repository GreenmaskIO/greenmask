package proto

import (
	"github.com/jackc/pgio"
)

const (
	OriginalPayloadCommandId = 'p'
)

type OriginalPayload struct {
	Payload []byte
}

func (ar *OriginalPayload) Decode(src []byte) error {
	ar.Payload = src
	return nil
}

func (ar *OriginalPayload) Encode(dst []byte) ([]byte, error) {
	dst = append(dst, byte(ar.CommandId()))
	dst = pgio.AppendInt32(dst, int32(4+len(ar.Payload)+1))
	dst = append(dst, ar.Payload...)
	return dst, nil
}

func (ar *OriginalPayload) CommandId() rune {
	return OriginalPayloadCommandId
}

func (ar *OriginalPayload) Backend() {}
