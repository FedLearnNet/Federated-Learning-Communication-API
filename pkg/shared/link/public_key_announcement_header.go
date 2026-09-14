package link

import (
	"encoding/binary"
	"errors"
	"fmt"

	util "fc_controller/pkg/shared/util"
)

// PublicKeyAnnouncementHeader stores metadata for a client announcing its public key.
type PublicKeyAnnouncementHeader struct {
	Cmd       byte
	Channel   RelayChannel
	PublicKey util.PublicKey
}

func NewPublicKeyAnnouncementHeader(channel RelayChannel, publicKey util.PublicKey) *PublicKeyAnnouncementHeader {
	return &PublicKeyAnnouncementHeader{
		Cmd:       CMD_SEND_PUBLIC_KEY,
		Channel:   channel,
		PublicKey: publicKey,
	}
}

// PublicKeyBundleEntry stores one client/public-key pair in a broadcast bundle.
type PublicKeyBundleEntry struct {
	ClientID  ClientID
	PublicKey util.PublicKey
}

// PublicKeyBundleHeader stores metadata for a global server broadcasting public keys.
type PublicKeyBundleHeader struct {
	Cmd        byte
	Channel    RelayChannel
	NumEntries uint32
	Entries    []PublicKeyBundleEntry
}

// ReadPublicKeyAnnouncementHeader reads a client public-key announcement from the connection.
//
// The wire format is:
//
//	byte cmd (CMD_SEND_PUBLIC_KEY)
//		Please note that this is not included in the READ as this is read by the caller to
//		Determine which header to read in
//	[32]byte channel
//	[32]byte publicKey
func ReadPublicKeyAnnouncementHeader(h TCPIOInterface, cmd byte) (*PublicKeyAnnouncementHeader, error) {
	header := &PublicKeyAnnouncementHeader{}
	var err error
	header.Cmd = cmd
	if header.Cmd != CMD_SEND_PUBLIC_KEY {
		return nil, fmt.Errorf("unsupported command: %d", header.Cmd)
	}

	if err = h.ReadBytes(header.Channel[:]); err != nil {
		return nil, err
	}
	if err = h.ReadBytes(header.PublicKey.CPoint[:]); err != nil {
		return nil, err
	}

	return header, nil
}

func (h *TCPIO) ReadPublicKeyAnnouncementHeader(cmd byte) (*PublicKeyAnnouncementHeader, error) {
	return ReadPublicKeyAnnouncementHeader(h, cmd)
}

// WritePublicKeyAnnouncementHeader writes a client public-key announcement to the connection.
func WritePublicKeyAnnouncementHeader(h TCPIOInterface, header *PublicKeyAnnouncementHeader) error {
	if header == nil {
		return errors.New("header is nil")
	}
	if header.Cmd != CMD_SEND_PUBLIC_KEY {
		return fmt.Errorf("unsupported command: %d", header.Cmd)
	}

	if err := h.WriteByte(header.Cmd); err != nil {
		return err
	}
	if err := h.WriteBytes(header.Channel[:]); err != nil {
		return err
	}
	if err := h.WriteBytes(header.PublicKey.CPoint[:]); err != nil {
		return err
	}

	return nil
}

func (h *TCPIO) WritePublicKeyAnnouncementHeader(header *PublicKeyAnnouncementHeader) error {
	return WritePublicKeyAnnouncementHeader(h, header)
}

// ReadPublicKeyBundleHeader reads a global public-key bundle from the connection.
//
// The wire format is:
//
//	byte cmd (CMD_SEND_PUBLIC_KEYS)
//		Please note that this is not included in the READ as this is read by the caller to
//		Determine which header to read in
//	[32]byte channel
//	[4]byte numEntries
//	Repeated numEntries times:
//		[8]byte clientID
//		[32]byte publicKey
func ReadPublicKeyBundleHeader(h TCPIOInterface, cmd byte) (*PublicKeyBundleHeader, error) {
	header := &PublicKeyBundleHeader{}
	var err error
	header.Cmd = cmd

	if header.Cmd != CMD_SEND_PUBLIC_KEYS {
		return nil, fmt.Errorf("unsupported command: %d", header.Cmd)
	}

	if err = h.ReadBytes(header.Channel[:]); err != nil {
		return nil, err
	}

	entryCountBytes := [4]byte{}
	if err = h.ReadBytes(entryCountBytes[:]); err != nil {
		return nil, err
	}
	header.NumEntries = binary.BigEndian.Uint32(entryCountBytes[:])
	header.Entries = make([]PublicKeyBundleEntry, 0, header.NumEntries)

	for i := uint32(0); i < header.NumEntries; i++ {
		entry := PublicKeyBundleEntry{}
		if err = h.ReadBytes(entry.ClientID[:]); err != nil {
			return nil, err
		}
		if err = h.ReadBytes(entry.PublicKey.CPoint[:]); err != nil {
			return nil, err
		}
		header.Entries = append(header.Entries, entry)
	}

	return header, nil
}

func (h *TCPIO) ReadPublicKeyBundleHeader(cmd byte) (*PublicKeyBundleHeader, error) {
	return ReadPublicKeyBundleHeader(h, cmd)
}

// WritePublicKeyBundleHeader writes a global public-key bundle to the connection.
func WritePublicKeyBundleHeader(h TCPIOInterface, header *PublicKeyBundleHeader) error {
	if header == nil {
		return errors.New("header is nil")
	}
	if header.Cmd != CMD_SEND_PUBLIC_KEYS {
		return fmt.Errorf("unsupported command: %d", header.Cmd)
	}
	if header.NumEntries != uint32(len(header.Entries)) {
		return fmt.Errorf("num entries mismatch: got %d, want %d", header.NumEntries, len(header.Entries))
	}

	if err := h.WriteByte(header.Cmd); err != nil {
		return err
	}
	if err := h.WriteBytes(header.Channel[:]); err != nil {
		return err
	}

	entryCountBytes := [4]byte{}
	binary.BigEndian.PutUint32(entryCountBytes[:], header.NumEntries)
	if err := h.WriteBytes(entryCountBytes[:]); err != nil {
		return err
	}

	for _, entry := range header.Entries {
		if err := h.WriteBytes(entry.ClientID[:]); err != nil {
			return err
		}
		if err := h.WriteBytes(entry.PublicKey.CPoint[:]); err != nil {
			return err
		}
	}

	return nil
}

func (h *TCPIO) WritePublicKeyBundleHeader(header *PublicKeyBundleHeader) error {
	return WritePublicKeyBundleHeader(h, header)
}
