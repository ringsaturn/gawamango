package protocol

import (
	"encoding/binary"
	"fmt"
)

// Command represents a MongoDB command
type Command struct {
	CommandName string                 `json:"command"`
	Database    string                 `json:"database"`
	Arguments   map[string]interface{} `json:"arguments"`
}

// ParseCommand parses a MongoDB message body to extract the command
func ParseCommand(opCode int32, body []byte) (*Command, error) {
	cmd := &Command{
		Arguments: make(map[string]interface{}),
	}

	// Only need to handle OP_MSG and OP_COMPRESSED.
	//
	// > Starting in MongoDB 5.1, OP_MSG and OP_COMPRESSED are the only supported opcodes to send requests to a MongoDB server.
	//
	// https://www.mongodb.com/docs/manual/legacy-opcodes/
	switch opCode {
	case OP_MSG:
		// OP_MSG format:
		// flags (int32)
		// sections (array of sections)
		// checksum (optional, uint32)

		if len(body) < 4 {
			return nil, fmt.Errorf("message too short")
		}

		// Skip flags
		body = body[4:]

		// First section is the command
		if len(body) < 1 {
			return nil, fmt.Errorf("no sections in message")
		}

		sectionType := body[0]
		body = body[1:]

		if sectionType != 0 {
			return nil, fmt.Errorf("unsupported section type: %d", sectionType)
		}

		// Parse BSON document
		doc, err := parseBSON(body)
		if err != nil {
			return nil, err
		}

		// Extract command name (first key in document)
		for k, v := range doc {
			cmd.CommandName = k
			cmd.Arguments[k] = v
			break
		}

		// Extract database name if present
		if db, ok := doc["$db"].(string); ok {
			cmd.Database = db
		}

	// TODO
	// case OP_COMPRESSED:
	// 	// OP_COMPRESSED format:
	// 	// flags (int32)
	// 	// sections (array of sections)
	// 	// checksum (optional, uint32)

	default:
		return nil, fmt.Errorf("unsupported opcode: %d", opCode)
	}

	return cmd, nil
}

// parseBSON parses a BSON document into a map
func parseBSON(data []byte) (map[string]interface{}, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("document too short")
	}

	// Get document length
	docLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if docLen > len(data) || docLen < 5 {
		return nil, fmt.Errorf("invalid document length")
	}

	// Work strictly within this BSON document to avoid bleeding into the
	// next section of the MongoDB message.
	data = data[:docLen]

	// Skip the leading int32 length so `data` now points at the first element.
	data = data[4:]
	result := make(map[string]interface{})

	for len(data) > 1 { // At least 1 byte for type + 1 byte for null terminator
		// Get element type
		elementType := data[0]
		data = data[1:]

		// Get element name (up to null terminator)
		nameEnd := 0
		for nameEnd < len(data) && data[nameEnd] != 0 {
			nameEnd++
		}
		if nameEnd >= len(data) {
			return nil, fmt.Errorf("invalid element name")
		}

		name := string(data[:nameEnd])
		data = data[nameEnd+1:]

		// Parse value based on type
		// BSON element types as described in https://bsonspec.org/spec.html:
		// 0x01: Double (64-bit floating point)
		// 0x02: String (UTF-8)
		// 0x03: Embedded document
		// 0x04: Array
		// 0x05: Binary data
		// 0x06: Undefined (deprecated)
		// 0x07: ObjectId
		// 0x08: Boolean
		// 0x09: DateTime (UTC datetime)
		// 0x0A: Null
		// 0x0B: Regular expression
		// 0x0C: DBPointer (deprecated)
		// 0x0D: JavaScript
		// 0x0E: Symbol (deprecated)
		// 0x0F: JavaScript with scope
		// 0x10: 32-bit integer
		// 0x11: Timestamp
		// 0x12: 64-bit integer
		// 0x13: 128-bit decimal
		// 0x7F: Max key
		// 0xFF: Min key
		switch elementType {
		case 0x00: // end‑of‑object (EOO) sentinel
			// Reached the end of this document — stop parsing.
			return result, nil
		case 0x01: // double
			if len(data) < 8 {
				return nil, fmt.Errorf("invalid double value")
			}
			result[name] = binary.LittleEndian.Uint64(data[:8])
			data = data[8:]
		case 0x02: // string
			if len(data) < 4 {
				return nil, fmt.Errorf("invalid string length")
			}
			strLen := int(binary.LittleEndian.Uint32(data[:4]))
			if len(data) < 4+strLen {
				return nil, fmt.Errorf("invalid string value")
			}
			result[name] = string(data[4 : 4+strLen-1]) // -1 for null terminator
			data = data[4+strLen:]
		case 0x03: // document
			docLen := int(binary.LittleEndian.Uint32(data[:4]))
			if len(data) < docLen {
				return nil, fmt.Errorf("invalid document length")
			}
			doc, err := parseBSON(data[:docLen])
			if err != nil {
				return nil, err
			}
			result[name] = doc
			data = data[docLen:]
		case 0x04: // array
			docLen := int(binary.LittleEndian.Uint32(data[:4]))
			if len(data) < docLen {
				return nil, fmt.Errorf("invalid array length")
			}
			arr, err := parseBSON(data[:docLen])
			if err != nil {
				return nil, err
			}
			result[name] = arr
			data = data[docLen:]
		case 0x05: // binary
			if len(data) < 5 {
				return nil, fmt.Errorf("invalid binary length")
			}
			binLen := int(binary.LittleEndian.Uint32(data[:4]))
			subType := data[4]
			if len(data) < 5+binLen {
				return nil, fmt.Errorf("invalid binary data")
			}
			// Binary subtypes:
			// 0x00: Generic binary
			// 0x01: Function
			// 0x02: Binary (old)
			// 0x03: UUID (old)
			// 0x04: UUID
			// 0x05: MD5
			// 0x06: Encrypted
			// 0x07: Column
			// 0x08: Sensitive
			// 0x09: Vector
			// 0x80: User defined
			result[name] = map[string]interface{}{
				"type":    "binary",
				"subtype": subType,
				"data":    data[5 : 5+binLen],
			}
			data = data[5+binLen:]
		case 0x06: // undefined (deprecated)
			result[name] = map[string]interface{}{
				"type": "undefined",
			}
		case 0x07: // objectId
			if len(data) < 12 {
				return nil, fmt.Errorf("invalid objectId length")
			}
			result[name] = map[string]interface{}{
				"type": "objectId",
				"data": data[:12],
			}
			data = data[12:]
		case 0x08: // boolean
			if len(data) < 1 {
				return nil, fmt.Errorf("invalid boolean value")
			}
			result[name] = data[0] != 0
			data = data[1:]
		case 0x09: // datetime
			if len(data) < 8 {
				return nil, fmt.Errorf("invalid datetime length")
			}
			millis := int64(binary.LittleEndian.Uint64(data[:8]))
			result[name] = map[string]interface{}{
				"type": "datetime",
				"data": millis,
			}
			data = data[8:]
		case 0x0A: // null
			result[name] = nil
		case 0x0B: // regex
			// Pattern
			patternEnd := 0
			for patternEnd < len(data) && data[patternEnd] != 0 {
				patternEnd++
			}
			if patternEnd >= len(data) {
				return nil, fmt.Errorf("invalid regex pattern")
			}
			pattern := string(data[:patternEnd])
			data = data[patternEnd+1:]

			// Options
			optionsEnd := 0
			for optionsEnd < len(data) && data[optionsEnd] != 0 {
				optionsEnd++
			}
			if optionsEnd >= len(data) {
				return nil, fmt.Errorf("invalid regex options")
			}
			options := string(data[:optionsEnd])
			data = data[optionsEnd+1:]

			result[name] = map[string]interface{}{
				"type":    "regex",
				"pattern": pattern,
				"options": options,
			}
		case 0x0C: // dbpointer (deprecated)
			if len(data) < 12 {
				return nil, fmt.Errorf("invalid dbpointer length")
			}
			// Skip namespace
			nsEnd := 0
			for nsEnd < len(data) && data[nsEnd] != 0 {
				nsEnd++
			}
			if nsEnd >= len(data) {
				return nil, fmt.Errorf("invalid dbpointer namespace")
			}
			data = data[nsEnd+1:]

			result[name] = map[string]interface{}{
				"type": "dbpointer",
				"data": data[:12],
			}
			data = data[12:]
		case 0x0D: // javascript
			if len(data) < 4 {
				return nil, fmt.Errorf("invalid javascript length")
			}
			jsLen := int(binary.LittleEndian.Uint32(data[:4]))
			if len(data) < 4+jsLen {
				return nil, fmt.Errorf("invalid javascript data")
			}
			result[name] = map[string]interface{}{
				"type": "javascript",
				"data": string(data[4 : 4+jsLen-1]), // -1 for null terminator
			}
			data = data[4+jsLen:]
		case 0x0E: // symbol (deprecated)
			if len(data) < 4 {
				return nil, fmt.Errorf("invalid symbol length")
			}
			symLen := int(binary.LittleEndian.Uint32(data[:4]))
			if len(data) < 4+symLen {
				return nil, fmt.Errorf("invalid symbol data")
			}
			result[name] = map[string]interface{}{
				"type": "symbol",
				"data": string(data[4 : 4+symLen-1]), // -1 for null terminator
			}
			data = data[4+symLen:]
		case 0x0F: // javascript with scope
			if len(data) < 4 {
				return nil, fmt.Errorf("invalid javascript with scope length")
			}
			totalLen := int(binary.LittleEndian.Uint32(data[:4]))
			if len(data) < totalLen {
				return nil, fmt.Errorf("invalid javascript with scope data")
			}

			// Parse javascript code
			jsLen := int(binary.LittleEndian.Uint32(data[4:8]))
			jsCode := string(data[8 : 8+jsLen-1]) // -1 for null terminator
			data = data[8+jsLen:]

			// Parse scope document
			scopeLen := int(binary.LittleEndian.Uint32(data[:4]))
			scope, err := parseBSON(data[:scopeLen])
			if err != nil {
				return nil, err
			}

			result[name] = map[string]interface{}{
				"type":  "javascript_with_scope",
				"code":  jsCode,
				"scope": scope,
			}
			data = data[scopeLen:]
		case 0x10: // int32
			if len(data) < 4 {
				return nil, fmt.Errorf("invalid int32 value")
			}
			result[name] = int32(binary.LittleEndian.Uint32(data[:4]))
			data = data[4:]
		case 0x11: // timestamp
			if len(data) < 8 {
				return nil, fmt.Errorf("invalid timestamp length")
			}
			inc := binary.LittleEndian.Uint32(data[:4])
			time := binary.LittleEndian.Uint32(data[4:8])
			result[name] = map[string]interface{}{
				"type": "timestamp",
				"inc":  inc,
				"time": time,
			}
			data = data[8:]
		case 0x12: // int64
			if len(data) < 8 {
				return nil, fmt.Errorf("invalid int64 value")
			}
			result[name] = int64(binary.LittleEndian.Uint64(data[:8]))
			data = data[8:]
		case 0x13: // decimal128
			if len(data) < 16 {
				return nil, fmt.Errorf("invalid decimal128 length")
			}
			result[name] = map[string]interface{}{
				"type": "decimal128",
				"data": data[:16],
			}
			data = data[16:]
		case 0xFF: // min key
			result[name] = map[string]interface{}{
				"type": "min_key",
			}
		case 0x7F: // max key
			result[name] = map[string]interface{}{
				"type": "max_key",
			}
		default:
			// 对于未知类型，直接跳过整个消息
			rawMsg := data
			fmt.Printf("Warning: skipping message with unknown BSON type: %d (0x%x)\n", elementType, elementType)
			fmt.Printf("Raw message: %x\n", rawMsg)
			return nil, nil
		}
	}

	return result, nil
}
