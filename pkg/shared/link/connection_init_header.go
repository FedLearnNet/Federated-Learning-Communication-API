package link

import (
	"errors"
	"fmt"

	util "fc_controller/pkg/shared/util"
)

// ConnectionSetupHeader stores metadata for the initial setup/auth handshake
// between local and global relay servers.
type ConnectionSetupHeader struct {
	Cmd       byte
	Channel   RelayChannel
	ClientID  ClientID
	ClientKey util.APIKey
}

func NewConnectionSetupHeader(channel RelayChannel, clientID ClientID, clientKey util.APIKey) *ConnectionSetupHeader {
	return &ConnectionSetupHeader{
		Cmd:       CMD_INIT_CONNECTION,
		Channel:   channel,
		ClientID:  clientID,
		ClientKey: clientKey,
	}
}

// ReadConnectionSetupHeader reads the setup/auth header from the connection.
//
// The wire format is:
//
//	byte MAGIC_BYTE_1 ('F')
//	byte MAGIC_BYTE_2 ('C')
//	byte cmd (CMD_INIT_CONNECTION)
//	[32]byte channel
//	[8]byte clientID
//	[APIKeyEncodedLen(APIKeyEntropyBytes)]byte clientKey
//
// Note: Caller is responsible for validating the semantic meaning of the fields.
func ReadConnectionSetupHeader(h TCPIOInterface) (*ConnectionSetupHeader, error) {
	header := &ConnectionSetupHeader{}
	var err error

	// Validate magic bytes
	if b, err := h.ReadByte(); err != nil {
		return nil, err
	} else if b != MAGIC_BYTE_1 {
		return nil, fmt.Errorf("invalid magic byte 1: expected %d, got %d", MAGIC_BYTE_1, b)
	}

	if b, err := h.ReadByte(); err != nil {
		return nil, err
	} else if b != MAGIC_BYTE_2 {
		return nil, fmt.Errorf("invalid magic byte 2: expected %d, got %d", MAGIC_BYTE_2, b)
	}

	if header.Cmd, err = h.ReadByte(); err != nil {
		return nil, err
	}
	if header.Cmd != CMD_INIT_CONNECTION {
		return nil, fmt.Errorf("unsupported command: %d", header.Cmd)
	}

	if err = h.ReadBytes(header.Channel[:]); err != nil {
		return nil, err
	}
	if err = h.ReadBytes(header.ClientID[:]); err != nil {
		return nil, err
	}

	clientKeyLen := util.APIKeyEncodedLen()
	clientKeyBytes := make([]byte, clientKeyLen)
	if err = h.ReadBytes(clientKeyBytes); err != nil {
		return nil, err
	}
	clientKey, err := util.DecodeAPIKeyBytes(clientKeyBytes)
	if err != nil {
		return nil, err
	}
	header.ClientKey = clientKey

	return header, nil
}

// WriteConnectionSetupResponse writes the relay server's authentication response
// after it has verified the client. This allows the client to verify the relay
// server's identity (mutual auth).
//
// Wire format:
//
//	byte MAGIC_BYTE_1 ('F')
//	byte MAGIC_BYTE_2 ('C')
//	byte CMD_INIT_CONNECTION
//	[APIKeyEncodedLen]byte relayKey
func WriteConnectionSetupResponse(h TCPIOInterface, relayKey util.APIKey) error {
	if err := h.WriteByte(MAGIC_BYTE_1); err != nil {
		return err
	}
	if err := h.WriteByte(MAGIC_BYTE_2); err != nil {
		return err
	}
	if err := h.WriteByte(CMD_INIT_CONNECTION); err != nil {
		return err
	}
	relayKeyBytes := relayKey.EncodeAPIKeybytes()
	if len(relayKeyBytes) != util.APIKeyEncodedLen() {
		return fmt.Errorf("invalid relay key length: got %d, expected %d", len(relayKeyBytes), util.APIKeyEncodedLen())
	}
	return h.WriteBytes(relayKeyBytes)
}

// ReadConnectionSetupResponse reads the relay server's authentication response
// so the client can verify the relay server's identity (mutual auth).
// The wire format is identical to WriteConnectionSetupResponse.
func ReadConnectionSetupResponse(h TCPIOInterface) (util.APIKey, error) {
	if b, err := h.ReadByte(); err != nil {
		return "", err
	} else if b != MAGIC_BYTE_1 {
		return "", fmt.Errorf("invalid magic byte 1 in relay response: expected %d, got %d", MAGIC_BYTE_1, b)
	}
	if b, err := h.ReadByte(); err != nil {
		return "", err
	} else if b != MAGIC_BYTE_2 {
		return "", fmt.Errorf("invalid magic byte 2 in relay response: expected %d, got %d", MAGIC_BYTE_2, b)
	}
	cmd, err := h.ReadByte()
	if err != nil {
		return "", err
	}
	if cmd != CMD_INIT_CONNECTION {
		return "", fmt.Errorf("unexpected command in relay response: %d", cmd)
	}
	relayKeyBytes := make([]byte, util.APIKeyEncodedLen())
	if err := h.ReadBytes(relayKeyBytes); err != nil {
		return "", err
	}
	return util.DecodeAPIKeyBytes(relayKeyBytes)
}

// WriteConnectionSetupHeader writes the setup/auth header to the connection.
// The wire format is identical to ReadConnectionSetupHeader for symmetry.
func WriteConnectionSetupHeader(h TCPIOInterface, header *ConnectionSetupHeader) error {
	if header == nil {
		return errors.New("header is nil")
	}
	if header.Cmd != CMD_INIT_CONNECTION {
		return fmt.Errorf("unsupported command: %d", header.Cmd)
	}

	// Write magic bytes for protocol validation
	if err := h.WriteByte(MAGIC_BYTE_1); err != nil {
		return err
	}
	if err := h.WriteByte(MAGIC_BYTE_2); err != nil {
		return err
	}

	if err := h.WriteByte(header.Cmd); err != nil {
		return err
	}
	if err := h.WriteBytes(header.Channel[:]); err != nil {
		return err
	}
	if err := h.WriteBytes(header.ClientID[:]); err != nil {
		return err
	}

	clientKeyBytes := header.ClientKey.EncodeAPIKeybytes()
	expectedClientKeyLen := util.APIKeyEncodedLen()
	if len(clientKeyBytes) != expectedClientKeyLen {
		return fmt.Errorf("invalid client key length: got %d, expected %d", len(clientKeyBytes), expectedClientKeyLen)
	}
	if err := h.WriteBytes(clientKeyBytes); err != nil {
		return err
	}

	return nil
}
