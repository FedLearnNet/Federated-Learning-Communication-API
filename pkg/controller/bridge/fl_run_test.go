package bridge

import (
	"testing"

	shared_enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
)

func makeTestClientID(id byte) shared.ClientID {
	result := shared.ClientID{}
	result[0] = id
	return result
}

func makeTestRelayChannel(name string) shared.RelayChannel {
	result := shared.RelayChannel{}
	copy(result[:], name)
	return result
}

func makeTestKeyPair(t *testing.T) (util.PrivateKey, util.PublicKey) {
	t.Helper()
	privKey, pubKey, err := util.GenerateKeyPair()
	if err != nil || privKey.PrivateKey == nil {
		t.Fatal("Failed to generate key pair")
	}
	return privKey, pubKey
}

func TestNewFlRunMeta_Success(t *testing.T) {
	channel := makeTestRelayChannel("test-channel")
	coordinatorID := makeTestClientID(1)
	ownID := makeTestClientID(2)
	ownPrivKey, ownPubKey := makeTestKeyPair(t)

	meta, err := NewFlRunMeta(
		channel, "secret-key", "relay-key", coordinatorID, ownID, false, 2,
		[]shared.ClientID{coordinatorID, ownID}, ownPubKey, ownPrivKey, shared_enums.AppVersionV2,
	)
	if err != nil {
		t.Fatalf("NewFlRunMeta failed: %v", err)
	}
	if meta.Channel != channel {
		t.Errorf("Channel mismatch: got %v, want %v", meta.Channel, channel)
	}
	if meta.OwnClientKey != "secret-key" {
		t.Errorf("OwnClientKey mismatch: got %s", meta.OwnClientKey)
	}
	if meta.CoordinatorID != coordinatorID {
		t.Errorf("CoordinatorID mismatch")
	}
	if meta.OwnClientId != ownID {
		t.Errorf("OwnClientId mismatch")
	}
	if meta.MaxNumberOfClients != 2 {
		t.Errorf("MaxNumberOfClients: got %d, want 2", meta.MaxNumberOfClients)
	}
	if meta.Version != 1 {
		t.Errorf("Version: got %d, want 1", meta.Version)
	}
	handle := NewFLRunHandle(meta)
	if handle.GetState() != shared_enums.StateInit {
		t.Errorf("Initial state: got %d, want StateInit", handle.GetState())
	}
	if pubKey, exists := meta.ClientId2PublicKey[ownID]; !exists {
		t.Error("Own client ID not in ClientId2PublicKey")
	} else if pubKey != ownPubKey {
		t.Error("Own public key mismatch")
	}
}

func TestNewFlRunMeta_EmptyChannel(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	_, err := NewFlRunMeta(
		shared.RelayChannel{}, "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	if err == nil {
		t.Error("Expected error for empty channel")
	}
}

func TestNewFlRunMeta_EmptyClientKey(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	_, err := NewFlRunMeta(
		makeTestRelayChannel("ch"), "", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	if err == nil {
		t.Error("Expected error for empty client key")
	}
}

func TestNewFlRunMeta_InvalidMaxClients(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	_, err := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 0,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	if err == nil {
		t.Error("Expected error for maxNumberOfClients=0")
	}
}

func TestNewFlRunMeta_MismatchedClientOrder(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	_, err := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1)}, // only 1 entry for maxClients=2
		pubKey, privKey, shared_enums.AppVersionV2,
	)
	if err == nil {
		t.Error("Expected error for mismatched ClientIDOrder length")
	}
}

func TestIsSetupComplete(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	// Single-client run (ourselves): should be immediately complete.
	ownID := makeTestClientID(1)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", ownID, ownID, true, 1,
		[]shared.ClientID{ownID}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	if !meta.IsSetupComplete() {
		t.Error("Expected setup complete for 1-of-1 clients known")
	}

	// 3-client run with only 1 known: incomplete.
	privKey2, pubKey2 := makeTestKeyPair(t)
	id1, id2, id3 := makeTestClientID(1), makeTestClientID(2), makeTestClientID(3)
	meta2, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", id1, id2, false, 3,
		[]shared.ClientID{id1, id2, id3}, pubKey2, privKey2, shared_enums.AppVersionV2,
	)
	if meta2.IsSetupComplete() {
		t.Error("Expected setup incomplete for 1-of-3 clients known")
	}
}

func TestCountKnownClients(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	id1, id2, id3 := makeTestClientID(1), makeTestClientID(2), makeTestClientID(3)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", id1, id2, false, 3,
		[]shared.ClientID{id1, id2, id3}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	if meta.CountKnownClients() != 1 {
		t.Errorf("Expected 1 known client, got %d", meta.CountKnownClients())
	}
	_, coordinatorPubKey := makeTestKeyPair(t)
	meta.ClientId2PublicKey[id1] = coordinatorPubKey
	if meta.CountKnownClients() != 2 {
		t.Errorf("Expected 2 known clients, got %d", meta.CountKnownClients())
	}
}

func TestMarkRunning(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	handle := NewFLRunHandle(meta)
	if !handle.MarkRunning() {
		t.Error("First MarkRunning should succeed")
	}
	if handle.GetState() != shared_enums.StateRunning {
		t.Errorf("Expected StateRunning, got %d", handle.GetState())
	}
	if handle.MarkRunning() {
		t.Error("Second MarkRunning should fail")
	}
}

func TestMarkError(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	handle := NewFLRunHandle(meta)
	handle.MarkRunning()
	success, _, _ := handle.MarkError()
	if !success {
		t.Error("MarkError should succeed from running state")
	}
	if handle.GetState() != shared_enums.StateError {
		t.Errorf("Expected StateError, got %d", handle.GetState())
	}
}

func TestMarkError_AfterFinished(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	handle := NewFLRunHandle(meta)
	handle.MarkRunning()
	_, _, _ = handle.MarkFinished()
	success, _, _ := handle.MarkError()
	if success {
		t.Error("MarkError should fail after StateFinished")
	}
}

func TestMarkFinished(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	handle := NewFLRunHandle(meta)
	handle.MarkRunning()
	success, _, _ := handle.MarkFinished()
	if !success {
		t.Error("MarkFinished should succeed from running state")
	}
	if handle.GetState() != shared_enums.StateFinished {
		t.Errorf("Expected StateFinished, got %d", handle.GetState())
	}
	success2, _, _ := handle.MarkFinished()
	if success2 {
		t.Error("Second MarkFinished should fail")
	}
}

func TestIsFinished(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	handle := NewFLRunHandle(meta)
	if handle.IsFinished() {
		t.Error("Should not be finished initially")
	}
	handle.MarkRunning()
	_, _, _ = handle.MarkFinished()
	if !handle.IsFinished() {
		t.Error("Should be finished after MarkFinished")
	}
}

func TestNilReceiver(t *testing.T) {
	var meta *FlRunMeta
	if meta.IsSetupComplete() {
		t.Error("IsSetupComplete nil")
	}
	if meta.CountKnownClients() != 0 {
		t.Error("CountKnownClients nil")
	}

	var handle *FLRunHandle
	if handle.GetState() != shared_enums.StateInit {
		t.Error("GetState nil")
	}
	if handle.MarkRunning() {
		t.Error("MarkRunning nil")
	}
	if ok, _, _ := handle.MarkError(); ok {
		t.Error("MarkError nil")
	}
	if ok, _, _ := handle.MarkFinished(); ok {
		t.Error("MarkFinished nil")
	}
	if handle.IsFinished() {
		t.Error("IsFinished nil")
	}
	if err := meta.Validate(); err == nil {
		t.Error("Validate nil should return error")
	}
}

func TestStateTransitions(t *testing.T) {
	privKey, pubKey := makeTestKeyPair(t)
	meta, _ := NewFlRunMeta(
		makeTestRelayChannel("ch"), "key", "relay-key", makeTestClientID(1), makeTestClientID(2), false, 2,
		[]shared.ClientID{makeTestClientID(1), makeTestClientID(2)}, pubKey, privKey, shared_enums.AppVersionV2,
	)
	handle := NewFLRunHandle(meta)
	if handle.GetState() != shared_enums.StateInit {
		t.Fatal("Initial state should be StateInit")
	}
	handle.MarkRunning()
	if handle.GetState() != shared_enums.StateRunning {
		t.Fatal("Expected StateRunning")
	}
	_, _, _ = handle.MarkFinished()
	if handle.GetState() != shared_enums.StateFinished {
		t.Fatal("Expected StateFinished")
	}
}
