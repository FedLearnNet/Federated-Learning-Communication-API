package link

import (
	util "fc_controller/pkg/shared/util"
	"testing"
)

func TestConnectionSetupHeaderRoundTrip(t *testing.T) {
	clientKey, err := util.GenerateAPIKey()
	if err != nil {
		t.Fatalf("generate API key: %v", err)
	}

	original := &ConnectionSetupHeader{
		Cmd:       CMD_INIT_CONNECTION,
		Channel:   NewChannel(),
		ClientID:  ClientID{1, 2, 3, 4, 5, 6, 7, 8},
		ClientKey: clientKey,
	}

	writer := NewMockTCPIO()
	if err := WriteConnectionSetupHeader(writer, original); err != nil {
		t.Fatalf("write setup header: %v", err)
	}

	reader := NewMockTCPIO()
	if _, err := reader.Buffer.Write(writer.Buffer.Bytes()); err != nil {
		t.Fatalf("copy mock bytes: %v", err)
	}

	decoded, err := ReadConnectionSetupHeader(reader)
	if err != nil {
		t.Fatalf("read setup header: %v", err)
	}

	if decoded.Cmd != original.Cmd {
		t.Fatalf("cmd mismatch: got %d want %d", decoded.Cmd, original.Cmd)
	}
	if decoded.Channel != original.Channel {
		t.Fatalf("channel mismatch")
	}
	if decoded.ClientID != original.ClientID {
		t.Fatalf("client id mismatch")
	}
	if decoded.ClientKey != original.ClientKey {
		t.Fatalf("client key mismatch")
	}
}
