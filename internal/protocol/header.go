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
	OP_MSG  = 2013
	OpQuery = 2004
	OpReply = 1
)

// OpcodeToName converts MongoDB wire protocol opcodes to human‑readable names.
// Only the opcodes used by modern servers are listed; others fall back to "UNKNOWN".
func OpcodeToName(op int32) string {
	switch op {
	case 1:
		return "OP_REPLY"
	case 2001:
		return "OP_UPDATE"
	case 2002:
		return "OP_INSERT"
	case 2003:
		return "RESERVED"
	case 2004:
		return "OP_QUERY"
	case 2005:
		return "OP_GET_MORE"
	case 2006:
		return "OP_DELETE"
	case 2007:
		return "OP_KILL_CURSORS"
	case 2012:
		return "OP_COMPRESSED"
	case 2013:
		return "OP_MSG"
	default:
		return "UNKNOWN"
	}
}
