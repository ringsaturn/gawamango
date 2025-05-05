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
	OpCode        int32
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
		OpCode:        int32(binary.LittleEndian.Uint32(data[12:16])),
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

// OpCode constants
const (
	OpMsg   = 2013
	OpQuery = 2004
	OpReply = 1
)
