package proto

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/jackc/pgproto3"
)

type FrontendMessage interface {
	Message
	Frontend()
}

type Frontend struct {
	cr pgproto3.ChunkReader
	w  io.Writer

	sessionId uuid.UUID
	jobId     int16
	version   int8

	// Backend flow
	authenticationOk    AuthenticationOk
	backendError        BackendError
	originalPayload     OriginalPayload
	validationRequest   ValidationRequest
	transformationStart TransformationStart
	transformationDone  TransformationDone
}

func (f *Frontend) Send(msg BackendMessage) error {
	data, err := msg.Encode(nil)
	if err != nil {
		return &CodecError{
			Msg: "cannot encode frontend message",
			Err: err,
		}
	}
	_, err = f.w.Write(data)
	return err
}

func (f *Frontend) Receive() (BackendMessage, error) {
	header, err := f.cr.Next(5)
	if err != nil {
		return nil, translateEOFtoErrUnexpectedEOF(err)
	}

	msgType := header[0]
	bodyLen := int(binary.BigEndian.Uint32(header[1:])) - 4
	if bodyLen < 0 {
		return nil, &CodecError{
			Msg: "negative body length",
		}
	}

	var msg BackendMessage
	switch msgType {
	case AuthenticationOkCommandId:
		msg = &f.authenticationOk
	case BackendErrorCommandId:
		msg = &f.backendError
	case OriginalPayloadCommandId:
		msg = &f.originalPayload
	case ValidationRequestCommandId:
		msg = &f.validationRequest
	case TransformationStartCommandId:
		msg = &f.transformationStart
	case TransformationDoneCommandId:
		msg = &f.transformationDone
	default:
		return nil, &CodecError{
			Msg: fmt.Sprintf("unknown backend message type: %c", msgType),
		}
	}

	msgBody, err := f.cr.Next(bodyLen)
	if err != nil {
		return nil, translateEOFtoErrUnexpectedEOF(err)
	}

	err = msg.Decode(msgBody)
	return msg, err
}
