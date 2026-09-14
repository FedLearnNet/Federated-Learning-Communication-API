// Contains the data-message header used between local and global relay,
// as well as methods for reading/writing it.
package link

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// DataPacketHeader stores metadata for one binary packet in transit between global and local relay.
// This is only used for data messages, anything related to the federated learning run setup
// uses another header
// Note: Validation of data types (CompressionMode, SMPCOperationByte, etc.) should be done
// by the caller to avoid import cycles. This struct stores raw byte values.
type DataPacketHeader struct {
	Cmd                     byte
	FromClientID            ClientID
	DestinationClientID     *ClientID
	SerializationMode       byte
	CompressionMode         byte
	MemoSize                byte // Memo = communicationId, this was renamed but not refactored TODO: Refactor to avoid confusion
	MemoBytes               []byte
	MemoString              string
	ToAggregatorNameSize    byte
	ToAggregatorNameBytes   []byte
	ToAggregatorName        string
	FromAggregatorNameSize  byte
	FromAggregatorNameBytes []byte
	FromAggregatorName      string
	SmpcExponentUsed        byte
	SmpcOperationUsed       byte
	ContentLength           uint64
}

// ReadDataHeader reads a packet header from the connection.
// The wire format is:
//
//	byte cmd (CMD_SEND_PLAIN_DATA || CMD_SEND_SMPC_AGG || CMD_SEND_SMPC_DATA || CMD_SEND_P2P_DATA)
//		Please note that this is not included in the READ as this is read by the caller to
//		Determine which header to read in
//	[8]byte FromClientID
//	[8]byte DestinationClientID
//	byte serializationMode
//	byte compressionMode
//	byte memosize
//	[memosize]byte memo
//	byte ToAggregatorNameSize
//	[ToAggregatorNameSize]byte ToAggregatorName
//	byte FromAggregatorNameSize
//	[FromAggregatorNameSize]byte FromAggregatorName
//	byte SmpcExponentUsed
//	byte SmpcOperationUsedByte
//	[8]byte contentLength
//
// Note: Caller is responsible for validating SerializationMode, CompressionMode, and SmpcOperationUsed
func ReadDataHeader(h TCPIOInterface, cmd byte) (*DataPacketHeader, error) {
	header := &DataPacketHeader{}
	var err error
	header.Cmd = cmd

	if !IsDataMessageCmd(header.Cmd) {
		return nil, fmt.Errorf("unsupported command: %d", header.Cmd)
	}
	// FROM CLIENT ID
	if err = h.ReadBytes(header.FromClientID[:]); err != nil {
		return nil, err
	}

	// DESTINATION CLIENT ID
	destinationClientID := ZERO_CLIENT_ID
	if err = h.ReadBytes(destinationClientID[:]); err != nil {
		return nil, err
	}
	header.DestinationClientID = &destinationClientID

	// SERIALIZATION MODE
	if header.SerializationMode, err = h.ReadByte(); err != nil {
		return nil, err
	}

	// COMPRESSION MODE
	if header.CompressionMode, err = h.ReadByte(); err != nil {
		return nil, err
	}

	// MEMO
	if header.MemoSize, err = h.ReadByte(); err != nil {
		return nil, err
	}
	if header.MemoSize > 0 {
		header.MemoBytes = make([]byte, header.MemoSize)
		if err = h.ReadBytes(header.MemoBytes); err != nil {
			return nil, err
		}
		header.MemoString = string(header.MemoBytes)
	} else {
		header.MemoBytes = make([]byte, 0)
		header.MemoString = ""
	}

	// TO AGGREGATOR NAME
	if header.ToAggregatorNameSize, err = h.ReadByte(); err != nil {
		return nil, err
	}
	if header.ToAggregatorNameSize > 0 {
		header.ToAggregatorNameBytes = make([]byte, header.ToAggregatorNameSize)
		if err = h.ReadBytes(header.ToAggregatorNameBytes); err != nil {
			return nil, err
		}
		header.ToAggregatorName = string(header.ToAggregatorNameBytes)
	} else {
		header.ToAggregatorNameBytes = make([]byte, 0)
		header.ToAggregatorName = ""
	}

	// FROM AGGREGATOR NAME
	if header.FromAggregatorNameSize, err = h.ReadByte(); err != nil {
		return nil, err
	}
	if header.FromAggregatorNameSize > 0 {
		header.FromAggregatorNameBytes = make([]byte, header.FromAggregatorNameSize)
		if err = h.ReadBytes(header.FromAggregatorNameBytes); err != nil {
			return nil, err
		}
		header.FromAggregatorName = string(header.FromAggregatorNameBytes)
	} else {
		header.FromAggregatorNameBytes = make([]byte, 0)
		header.FromAggregatorName = ""
	}

	// SMPC PROPERTIES
	if header.SmpcExponentUsed, err = h.ReadByte(); err != nil {
		return nil, err
	}

	if header.SmpcOperationUsed, err = h.ReadByte(); err != nil {
		return nil, err
	}

	// CONTENT LENGTH (8 bytes)
	contentLength := [8]byte{}
	if err = h.ReadBytes(contentLength[:]); err != nil {
		return nil, err
	}
	header.ContentLength = binary.BigEndian.Uint64(contentLength[:])

	return header, nil
}

// WriteHeader writes a packet header to the connection.
// The wire format is identical to ReadDataHeader for symmetry:
//
//	byte cmd (CMD_SEND_PLAIN_DATA || CMD_SEND_SMPC_AGG || CMD_SEND_SMPC_DATA || CMD_SEND_P2P_DATA)
//	[8]byte FromClientID
//	[8]byte DestinationClientID
//	byte serializationMode
//	byte compressionMode
//	byte memosize
//	[memosize]byte memo
//	byte ToAggregatorNameSize
//	[ToAggregatorNameSize]byte ToAggregatorName
//	byte FromAggregatorNameSize
//	[FromAggregatorNameSize]byte FromAggregatorName
//	byte SmpcExponentUsed
//	byte SmpcOperationUsedByte
//
// The message content should be written separately after this header:
//
//	[8]byte contentLength
//	[contentLength]byte message
//
// Note: This function does NOT write the contentLength field. That must be written
// separately by the caller before writing the actual message data.
//
// Note: Caller is responsible for validating that SerializationMode, CompressionMode,
// and SmpcOperationUsed have valid values.
func WriteHeader(h TCPIOInterface, header *DataPacketHeader) error {
	if header == nil {
		return errors.New("header is nil")
	}

	// CMD (always)
	if err := h.WriteByte(header.Cmd); err != nil {
		return err
	}

	// FROM CLIENT ID (always) - written for symmetry, global relay can discard if needed
	if err := h.WriteBytes(header.FromClientID[:]); err != nil {
		return err
	}

	// DESTINATION - always write (8 bytes, nil means zeros)
	if header.DestinationClientID != nil {
		if err := h.WriteBytes((*header.DestinationClientID)[:]); err != nil {
			return err
		}
	} else {
		// Write zero destination clientID
		if err := h.WriteBytes(make([]byte, 8)); err != nil {
			return err
		}
	}

	// SERIALIZATION MODE (always)
	if err := h.WriteByte(header.SerializationMode); err != nil {
		return err
	}

	// COMPRESSION MODE (always)
	if err := h.WriteByte(header.CompressionMode); err != nil {
		return err
	}

	// MEMO (might have size 0)
	if err := h.WriteByte(header.MemoSize); err != nil {
		return err
	}
	if header.MemoSize > 0 {
		if err := h.WriteBytes(header.MemoBytes); err != nil {
			return err
		}
	}

	// TO AGGREGATOR NAME (might have size 0)
	if err := h.WriteByte(header.ToAggregatorNameSize); err != nil {
		return err
	}
	if header.ToAggregatorNameSize > 0 {
		if err := h.WriteBytes(header.ToAggregatorNameBytes); err != nil {
			return err
		}
	}

	// FROM AGGREGATOR NAME (might have size 0)
	if err := h.WriteByte(header.FromAggregatorNameSize); err != nil {
		return err
	}
	if header.FromAggregatorNameSize > 0 {
		if err := h.WriteBytes(header.FromAggregatorNameBytes); err != nil {
			return err
		}
	}

	// SMPC EXPONENT
	if err := h.WriteByte(header.SmpcExponentUsed); err != nil {
		return err
	}

	// SMPC OPERATION
	if err := h.WriteByte(header.SmpcOperationUsed); err != nil {
		return err
	}

	return nil
}

// WriteContentLength writes the 8-byte content length field.
// This is called after WriteHeader and before writing the actual message content.
func WriteContentLength(h TCPIOInterface, contentLength uint64) error {
	chunkSize := [8]byte{}
	binary.BigEndian.PutUint64(chunkSize[:], contentLength)
	return h.WriteBytes(chunkSize[:])
}

func IsDataMessageCmd(cmd byte) bool {
	return cmd == CMD_SEND_PLAIN_DATA || cmd == CMD_SEND_SMPC_AGG || cmd == CMD_SEND_SMPC_DATA || cmd == CMD_SEND_P2P_DATA
}
