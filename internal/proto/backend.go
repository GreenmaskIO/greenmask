package proto

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/jackc/pgproto3"
)

// Transformer interaction protocol
// Useful links: https://www.sobyte.net/post/2022-03/go-block-tcp-parse/
// You need to write:
// Seq diagram for transformers interaction in cases:
//	1. Interaction with PIPE (stdin, stderr)
//		1.1 just send message and receive the result
//		1.2 validation
//  2. Interaction with UNIX SOCKET
//		2.1 Initialisation
//		2.2 Validation
//		2.3 Transformation interaction
//		2.4 Termination

// Protocol design
// * Start TCP server on the port
// * Provide sessionId to the transformer and job id
// * Worker initialise the transformer
// * Transformer connects to the server process via Unix Socket, provides SessionId and WorkerId
// * Server checks the SessionId and WorkerId
// * Server gets the key from WorkerMap and retrieve channel from the map
// * Server sends initialised backend

// Package:
//	- PackageId (1b)
//  - Length (4b)
//  - Payload (len(Length))

type BackendMessage interface {
	Message
	Backend()
}

type Backend struct {
	cr pgproto3.ChunkReader
	w  io.Writer

	sessionId uuid.UUID
	jobId     int16
	version   int8

	// Frontend messages flow
	authenticationRequest AuthenticationRequest
	validationDone        ValidationDone
	transformedPayload    TransformedPayload
	frontendError         FrontendError
}

func (b *Backend) Send(msg BackendMessage) error {
	data, err := msg.Encode(nil)
	if err != nil {
		return &CodecError{
			Msg: "cannot encode backend message",
			Err: err,
		}
	}
	_, err = b.w.Write(data)
	return err
}

func (b *Backend) Receive() (FrontendMessage, error) {
	header, err := b.cr.Next(5)
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

	var msg FrontendMessage
	switch msgType {
	case AuthenticationRequestCommandId:
		msg = &b.authenticationRequest
	case ValidationRequestCommandId:
		msg = &b.validationDone
	case TransformedPayloadCommandId:
		msg = &b.transformedPayload
	case FrontendErrorCommandId:
		msg = &b.frontendError
	default:
		return nil, &CodecError{
			Msg: fmt.Sprintf("unknown frontend message type: %c", msgType),
		}
	}

	msgBody, err := b.cr.Next(bodyLen)
	if err != nil {
		return nil, translateEOFtoErrUnexpectedEOF(err)
	}

	err = msg.Decode(msgBody)
	return msg, err
}
