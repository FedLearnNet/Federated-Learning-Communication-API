package flrun

import (
	"testing"

	"fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
)

func makeClientID(b byte) shared.ClientID {
	id := shared.ClientID{}
	id[0] = b
	return id
}

func makeKeyPair(t *testing.T) (util.PrivateKey, util.PublicKey) {
	t.Helper()
	priv, pub, err := util.GenerateKeyPair()
	if err != nil || priv.PrivateKey == nil {
		t.Fatal("failed to generate key pair")
	}
	return priv, pub
}

func makeChannel(name string) shared.RelayChannel {
	ch := shared.RelayChannel{}
	copy(ch[:], name)
	return ch
}

func TestValidateClientOrderAgainstMap(t *testing.T) {
	id1, id2 := makeClientID(1), makeClientID(2)
	_, pubKey1 := makeKeyPair(t)
	_, pubKey2 := makeKeyPair(t)
	_, pubKey3 := makeKeyPair(t)

	order := []shared.ClientID{id1, id2}
	validMap := map[shared.ClientID]util.PublicKey{id1: pubKey1}
	if err := validateClientOrderAgainstMap(order, validMap); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	dupOrder := []shared.ClientID{id1, id1}
	if err := validateClientOrderAgainstMap(dupOrder, validMap); err == nil {
		t.Error("Expected error for duplicate in order")
	}

	extraMap := map[shared.ClientID]util.PublicKey{
		id1: pubKey1, id2: pubKey2, makeClientID(3): pubKey3,
	}
	if err := validateClientOrderAgainstMap(order, extraMap); err == nil {
		t.Error("Expected error for client in map not in order")
	}
}

func TestValidateClientMapAgainstOrder(t *testing.T) {
	id1, id2, id3 := makeClientID(1), makeClientID(2), makeClientID(3)
	_, pubKey1 := makeKeyPair(t)
	_, pubKey2 := makeKeyPair(t)
	_, pubKey99 := makeKeyPair(t)

	order := []shared.ClientID{id1, id2, id3}
	validMap := map[shared.ClientID]util.PublicKey{id1: pubKey1, id2: pubKey2}
	if err := validateClientMapAgainstOrder(validMap, order); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	invalidMap := map[shared.ClientID]util.PublicKey{
		id1: pubKey1, makeClientID(99): pubKey99,
	}
	if err := validateClientMapAgainstOrder(invalidMap, order); err == nil {
		t.Error("Expected error for client not in order")
	}
}

func TestFlRun_IsSetupComplete_V1(t *testing.T) {
	// v1: coordinator is also in ClientIDOrder
	coordID := makeClientID(1)
	_, pubKey := makeKeyPair(t)

	fr := NewFlRun(makeChannel("ch"), coordID, []shared.ClientID{coordID}, 1, "", enums.AppVersionV1)
	if fr.IsSetupComplete() {
		t.Error("should not be complete before any key is set")
	}
	if err := fr.SetPublicKey(coordID, pubKey); err != nil {
		t.Fatalf("SetPublicKey: %v", err)
	}
	// In v1, coordinator key goes to both CoordinatorPublicKey and ClientId2PublicKey.
	if !fr.IsSetupComplete() {
		t.Error("expected setup complete after coordinator (v1) key set")
	}
}

func TestFlRun_IsSetupComplete_V2(t *testing.T) {
	// v2: coordinator is NOT in ClientIDOrder; two separate clients are.
	coordID := makeClientID(0)
	id1, id2 := makeClientID(1), makeClientID(2)
	_, coordPub := makeKeyPair(t)
	_, pub1 := makeKeyPair(t)
	_, pub2 := makeKeyPair(t)

	fr := NewFlRun(makeChannel("ch"), coordID, []shared.ClientID{id1, id2}, 2, "", enums.AppVersionV2)
	_ = fr.SetPublicKey(coordID, coordPub) // sets CoordinatorPublicKey only
	_ = fr.SetPublicKey(id1, pub1)
	if fr.IsSetupComplete() {
		t.Error("should not be complete with only 1 of 2 client keys")
	}
	_ = fr.SetPublicKey(id2, pub2)
	if !fr.IsSetupComplete() {
		t.Error("expected complete after all client keys and coordinator key set")
	}
}

func TestFlRun_Validate(t *testing.T) {
	coordID := makeClientID(1)
	id2 := makeClientID(2)

	fr := NewFlRun(makeChannel("ch"), coordID, []shared.ClientID{coordID, id2}, 2, "", enums.AppVersionV2)
	if err := fr.Validate(); err != nil {
		t.Errorf("expected valid FlRun, got: %v", err)
	}

	empty := &FlRunBase{}
	if err := empty.Validate(); err == nil {
		t.Error("expected error for empty FlRun")
	}

	var nilFr *FlRunBase
	if err := nilFr.Validate(); err == nil {
		t.Error("expected error for nil FlRun")
	}
}
