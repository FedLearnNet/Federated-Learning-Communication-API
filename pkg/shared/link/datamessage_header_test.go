package link

import (
	"bytes"
	"testing"
)

func TestDataPacketHeaderRoundTrip(t *testing.T) {
	destination := ClientID{8, 7, 6, 5, 4, 3, 2, 1}
	original := &DataPacketHeader{
		Cmd:                     CMD_SEND_SMPC_DATA,
		FromClientID:            ClientID{1, 2, 3, 4, 5, 6, 7, 8},
		DestinationClientID:     &destination,
		SerializationMode:       11,
		CompressionMode:         22,
		MemoSize:                byte(len("memo")),
		MemoBytes:               []byte("memo"),
		MemoString:              "memo",
		ToAggregatorNameSize:    byte(len("to_aggregator")),
		ToAggregatorNameBytes:   []byte("to_aggregator"),
		ToAggregatorName:        "to_aggregator",
		FromAggregatorNameSize:  byte(len("from_aggregator")),
		FromAggregatorNameBytes: []byte("from_aggregator"),
		FromAggregatorName:      "from_aggregator",
		SmpcExponentUsed:        3,
		SmpcOperationUsed:       SMPC_OP_ADD,
		ContentLength:           987654321,
	}

	io := NewMockTCPIO()
	if err := WriteHeader(io, original); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if err := WriteContentLength(io, original.ContentLength); err != nil {
		t.Fatalf("write content length: %v", err)
	}

	cmd, err := io.readByte()
	if err != nil {
		t.Fatalf("read cmd: %v", err)
	}
	decoded, err := ReadDataHeader(io, cmd)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}

	if decoded.Cmd != original.Cmd {
		t.Fatalf("cmd mismatch: got %d want %d", decoded.Cmd, original.Cmd)
	}
	if decoded.FromClientID != original.FromClientID {
		t.Fatalf("from client id mismatch")
	}
	if decoded.DestinationClientID == nil {
		t.Fatal("destination client id is nil")
	}
	if *decoded.DestinationClientID != *original.DestinationClientID {
		t.Fatalf("destination client id mismatch")
	}
	if decoded.SerializationMode != original.SerializationMode {
		t.Fatalf("serialization mode mismatch: got %d want %d", decoded.SerializationMode, original.SerializationMode)
	}
	if decoded.CompressionMode != original.CompressionMode {
		t.Fatalf("compression mode mismatch: got %d want %d", decoded.CompressionMode, original.CompressionMode)
	}
	if decoded.MemoSize != original.MemoSize || !bytes.Equal(decoded.MemoBytes, original.MemoBytes) || decoded.MemoString != original.MemoString {
		t.Fatal("memo mismatch")
	}
	if decoded.ToAggregatorNameSize != original.ToAggregatorNameSize || !bytes.Equal(decoded.ToAggregatorNameBytes, original.ToAggregatorNameBytes) || decoded.ToAggregatorName != original.ToAggregatorName {
		t.Fatal("to aggregator name mismatch")
	}
	if decoded.FromAggregatorNameSize != original.FromAggregatorNameSize || !bytes.Equal(decoded.FromAggregatorNameBytes, original.FromAggregatorNameBytes) || decoded.FromAggregatorName != original.FromAggregatorName {
		t.Fatal("from aggregator name mismatch")
	}
	if decoded.SmpcExponentUsed != original.SmpcExponentUsed {
		t.Fatalf("smpc exponent mismatch: got %d want %d", decoded.SmpcExponentUsed, original.SmpcExponentUsed)
	}
	if decoded.SmpcOperationUsed != original.SmpcOperationUsed {
		t.Fatalf("smpc operation mismatch: got %d want %d", decoded.SmpcOperationUsed, original.SmpcOperationUsed)
	}
	if decoded.ContentLength != original.ContentLength {
		t.Fatalf("content length mismatch: got %d want %d", decoded.ContentLength, original.ContentLength)
	}
}

func TestDataPacketHeaderRoundTripZeroValues(t *testing.T) {
	original := &DataPacketHeader{
		Cmd:                    CMD_SEND_PLAIN_DATA,
		FromClientID:           ClientID{9, 8, 7, 6, 5, 4, 3, 2},
		SerializationMode:      0,
		CompressionMode:        0,
		MemoSize:               0,
		ToAggregatorNameSize:   0,
		FromAggregatorNameSize: 0,
		SmpcExponentUsed:       0,
		SmpcOperationUsed:      0,
		ContentLength:          0,
	}

	io := NewMockTCPIO()
	if err := WriteHeader(io, original); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if err := WriteContentLength(io, original.ContentLength); err != nil {
		t.Fatalf("write content length: %v", err)
	}

	cmd, err := io.readByte()
	if err != nil {
		t.Fatalf("read cmd: %v", err)
	}
	decoded, err := ReadDataHeader(io, cmd)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}

	if decoded.Cmd != original.Cmd {
		t.Fatalf("cmd mismatch: got %d want %d", decoded.Cmd, original.Cmd)
	}
	if decoded.DestinationClientID == nil {
		t.Fatal("destination client id is nil")
	}
	if *decoded.DestinationClientID != ZERO_CLIENT_ID {
		t.Fatalf("destination client id mismatch: got %v want zero value", *decoded.DestinationClientID)
	}
	if decoded.MemoSize != 0 || len(decoded.MemoBytes) != 0 || decoded.MemoString != "" {
		t.Fatal("memo should be empty")
	}
	if decoded.ToAggregatorNameSize != 0 || len(decoded.ToAggregatorNameBytes) != 0 || decoded.ToAggregatorName != "" {
		t.Fatal("to aggregator name should be empty")
	}
	if decoded.FromAggregatorNameSize != 0 || len(decoded.FromAggregatorNameBytes) != 0 || decoded.FromAggregatorName != "" {
		t.Fatal("from aggregator name should be empty")
	}
	if decoded.ContentLength != 0 {
		t.Fatalf("content length mismatch: got %d want 0", decoded.ContentLength)
	}
}
