package link

import (
	util "fc_controller/pkg/shared/util"
	"testing"
)

func TestPublicKeyAnnouncementHeaderRoundTrip(t *testing.T) {
	original := &PublicKeyAnnouncementHeader{
		Cmd:       CMD_SEND_PUBLIC_KEY,
		Channel:   NewChannel(),
		PublicKey: util.PublicKey{},
	}
	for i := range original.PublicKey.CPoint {
		original.PublicKey.CPoint[i] = byte(255 - i)
	}

	io := NewMockTCPIO()
	if err := WritePublicKeyAnnouncementHeader(io, original); err != nil {
		t.Fatalf("write announcement header: %v", err)
	}

	cmd, err := io.readByte()
	if err != nil {
		t.Fatalf("read cmd: %v", err)
	}
	decoded, err := ReadPublicKeyAnnouncementHeader(io, cmd)
	if err != nil {
		t.Fatalf("read announcement header: %v", err)
	}

	if decoded.Cmd != original.Cmd || decoded.Channel != original.Channel || decoded.PublicKey != original.PublicKey {
		t.Fatal("announcement header round-trip mismatch")
	}
}

func TestPublicKeyBundleHeaderRoundTrip(t *testing.T) {
	entry1 := PublicKeyBundleEntry{ClientID: ClientID{1, 1, 1, 1, 1, 1, 1, 1}, PublicKey: util.PublicKey{}}
	entry2 := PublicKeyBundleEntry{ClientID: ClientID{2, 2, 2, 2, 2, 2, 2, 2}, PublicKey: util.PublicKey{}}
	for i := range entry1.PublicKey.CPoint {
		entry1.PublicKey.CPoint[i] = byte(i)
		entry2.PublicKey.CPoint[i] = byte(31 - i)
	}

	original := &PublicKeyBundleHeader{
		Cmd:        CMD_SEND_PUBLIC_KEYS,
		Channel:    NewChannel(),
		NumEntries: 2,
		Entries:    []PublicKeyBundleEntry{entry1, entry2},
	}

	io := NewMockTCPIO()
	if err := WritePublicKeyBundleHeader(io, original); err != nil {
		t.Fatalf("write bundle header: %v", err)
	}

	cmd, err := io.readByte()
	if err != nil {
		t.Fatalf("read cmd: %v", err)
	}
	decoded, err := ReadPublicKeyBundleHeader(io, cmd)
	if err != nil {
		t.Fatalf("read bundle header: %v", err)
	}

	if decoded.Cmd != original.Cmd || decoded.Channel != original.Channel || decoded.NumEntries != original.NumEntries {
		t.Fatal("bundle header metadata mismatch")
	}
	if len(decoded.Entries) != len(original.Entries) {
		t.Fatal("bundle entry count mismatch")
	}
	for i := range original.Entries {
		if decoded.Entries[i] != original.Entries[i] {
			t.Fatalf("bundle entry %d mismatch", i)
		}
	}
}
