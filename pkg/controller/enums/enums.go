package enums

import (
	"encoding/json"
	"fmt"
	"strings"
)

type CompressionMode string

const (
	CompressionNone        CompressionMode = "none"
	CompressionGzip        CompressionMode = "gzip"
	CompressionZstd        CompressionMode = "zstd"
	DefaultCompressionMode CompressionMode = CompressionNone
	// Make sure this is the same than DefaultCompressionModeByte
)

type CompressionModeByte byte

const (
	CompressionNoneByte        CompressionModeByte = 0
	CompressionGzipByte        CompressionModeByte = 1
	CompressionZstdByte        CompressionModeByte = 2
	DefaultCompressionModeByte CompressionModeByte = CompressionNoneByte
	// Make sure this is the same than DefaultCompressionMode
)

// ToByte converts a compression mode to its wire-format byte representation.
func (m CompressionMode) ToByte() (CompressionModeByte, error) {
	switch m {
	case CompressionNone:
		return CompressionNoneByte, nil
	case CompressionGzip:
		return CompressionGzipByte, nil
	case CompressionZstd:
		return CompressionZstdByte, nil
	default:
		return 0, fmt.Errorf("unsupported compression mode: %s", m)
	}
}

type SerializationMode string

const (
	SerializationNone        SerializationMode = "none"
	SerializationJSON        SerializationMode = "json"
	SerializationCBOR        SerializationMode = "cbor"
	SerializationPickle      SerializationMode = "pickle" // App v1 uses pickle for non smpc/dp/p2p messages
	SerializationGolang      SerializationMode = "golang" // in-process Go interface, never sent on the wire
	DefaultSerializationMode SerializationMode = SerializationCBOR
	// Make sure this is the same than DefaultSerializationModeByte
)

type SerializationModeByte byte

const (
	SerializationNoneByte        SerializationModeByte = 0
	SerializationJSONByte        SerializationModeByte = 1
	SerializationCBORByte        SerializationModeByte = 2
	SerializationPickleByte      SerializationModeByte = 3 // App v1 uses pickle for non smpc/dp/p2p messages
	SerializationGolangByte      SerializationModeByte = 4 // in-process Go interface, never sent on the wire
	DefaultSerializationModeByte SerializationModeByte = SerializationCBORByte
	// Make sure this is the same than DefaultSerializationMode
)

// ToByte converts a serialization mode to its wire-format byte representation.
func (m SerializationMode) ToByte() (SerializationModeByte, error) {
	switch m {
	case SerializationNone:
		return SerializationNoneByte, nil
	case SerializationJSON:
		return SerializationJSONByte, nil
	case SerializationCBOR:
		return SerializationCBORByte, nil
	case SerializationPickle:
		return SerializationPickleByte, nil
	case SerializationGolang:
		return SerializationGolangByte, nil
	default:
		return 0, fmt.Errorf("unsupported serialization mode: %s", m)
	}
}

// MarshalJSON encodes SerializationMode as a JSON string.
func (m SerializationMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(m))
}

// UnmarshalJSON decodes a JSON string into SerializationMode with validation.
func (m *SerializationMode) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := NormalizeSerializationMode(s)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

func NormalizeSerializationMode(serialization string) (SerializationMode, error) {
	s := strings.ToLower(strings.TrimSpace(serialization))
	if s == "" {
		s = string(SerializationNone)
	}

	sm := SerializationMode(s)
	switch sm {
	case SerializationNone, SerializationJSON, SerializationCBOR, SerializationPickle:
		return sm, nil
	default:
		return "", fmt.Errorf("unsupported serialization mode: %s", serialization)
	}
}

func NormalizeSerializationModeByte(serializationByte SerializationModeByte) (SerializationMode, error) {
	switch serializationByte {
	case SerializationNoneByte:
		return SerializationNone, nil
	case SerializationJSONByte:
		return SerializationJSON, nil
	case SerializationCBORByte:
		return SerializationCBOR, nil
	case SerializationPickleByte:
		return SerializationPickle, nil
	case SerializationGolangByte:
		return SerializationGolang, nil
	default:
		return "", fmt.Errorf("unsupported serialization mode byte: %d", serializationByte)
	}
}

func NormalizeCompressionMode(compression string) (CompressionMode, error) {
	c := strings.ToLower(strings.TrimSpace(compression))
	if c == "" {
		c = string(CompressionNone)
	}

	cm := CompressionMode(c)
	switch cm {
	case CompressionNone, CompressionGzip, CompressionZstd:
		return cm, nil
	default:
		return "", fmt.Errorf("unsupported compression mode: %s", compression)
	}
}

func NormalizeCompressionModeByte(compressionByte CompressionModeByte) (CompressionMode, error) {
	switch compressionByte {
	case CompressionNoneByte:
		return CompressionNone, nil
	case CompressionGzipByte:
		return CompressionGzip, nil
	case CompressionZstdByte:
		return CompressionZstd, nil
	default:
		return "", fmt.Errorf("unsupported compression mode byte: %d", compressionByte)
	}
}

type SMPCOperation string

const (
	SMPCOperationNone     SMPCOperation = "none"
	SMPCOperationAdd      SMPCOperation = "add"
	SMPCOperationMultiply SMPCOperation = "multiply"
	DefaultSMPCOperation  SMPCOperation = SMPCOperationAdd
)

type SMPCOperationByte byte

const (
	SMPCOperationNoneByte     SMPCOperationByte = 0
	SMPCOperationAddByte      SMPCOperationByte = 1
	SMPCOperationMultiplyByte SMPCOperationByte = 2
	DefaultSMPCOperationByte  SMPCOperationByte = SMPCOperationAddByte
)

// ToByte converts an SMPC operation to its wire-format byte representation.
func (o SMPCOperation) ToByte() (SMPCOperationByte, error) {
	switch o {
	case SMPCOperationNone:
		return SMPCOperationNoneByte, nil
	case SMPCOperationAdd:
		return SMPCOperationAddByte, nil
	case SMPCOperationMultiply:
		return SMPCOperationMultiplyByte, nil
	default:
		return 0, fmt.Errorf("unsupported SMPC operation: %s", o)
	}
}

func NormalizeSMPCOperation(operation string) (SMPCOperation, error) {
	o := strings.ToLower(strings.TrimSpace(operation))
	if o == "" {
		o = string(DefaultSMPCOperation)
	}

	op := SMPCOperation(o)
	switch op {
	case SMPCOperationAdd, SMPCOperationMultiply:
		return op, nil
	default:
		return "", fmt.Errorf("unsupported SMPC operation: %s", operation)
	}
}

func NormalizeSMPCOperationByte(operationByte SMPCOperationByte) (SMPCOperation, error) {
	switch operationByte {
	case SMPCOperationNoneByte:
		return SMPCOperationNone, nil
	case SMPCOperationAddByte:
		return SMPCOperationAdd, nil
	case SMPCOperationMultiplyByte:
		return SMPCOperationMultiply, nil
	default:
		return "", fmt.Errorf("unsupported SMPC operation byte: %d", operationByte)
	}
}
