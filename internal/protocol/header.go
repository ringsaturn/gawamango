package protocol

import (
	"encoding/binary"
	"errors"
)

// MsgHeader represents the MongoDB wire protocol message header
type MsgHeader struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        OpCode
}

// ParseHeader parses a MongoDB wire protocol message header from a byte slice
func ParseHeader(data []byte) (*MsgHeader, error) {
	if len(data) < 16 {
		return nil, errors.New("header too short")
	}

	header := &MsgHeader{
		MessageLength: int32(binary.LittleEndian.Uint32(data[0:4])),
		RequestID:     int32(binary.LittleEndian.Uint32(data[4:8])),
		ResponseTo:    int32(binary.LittleEndian.Uint32(data[8:12])),
		OpCode:        OpCode(binary.LittleEndian.Uint32(data[12:16])),
	}

	return header, nil
}

// SerializeHeader converts a MsgHeader to a byte slice
func SerializeHeader(header *MsgHeader) []byte {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint32(data[0:4], uint32(header.MessageLength))
	binary.LittleEndian.PutUint32(data[4:8], uint32(header.RequestID))
	binary.LittleEndian.PutUint32(data[8:12], uint32(header.ResponseTo))
	binary.LittleEndian.PutUint32(data[12:16], uint32(header.OpCode))
	return data
}

// Only need to handle OP_MSG and OP_COMPRESSED.
//
// > Starting in MongoDB 5.1, OP_MSG and OP_COMPRESSED are the only supported opcodes to send requests to a MongoDB server.
//
// https://www.mongodb.com/docs/manual/legacy-opcodes/
type OpCode int32

const (
	OP_QUERY      OpCode = 2004 // It's "deprecated" in doc, but still used in hello command when client init.
	OP_COMPRESSED OpCode = 2012
	OP_MSG        OpCode = 2013
)
