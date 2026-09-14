package util

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestParseCPointFromHex(t *testing.T) {
	_, pub, _ := GenerateKeyPair()
	r2, err := parseCPointFromHexStrict(pub.Hex())
	if err != nil {
		t.Fatal(err)
	}
	if !pub.CPoint.Equal(r2) {
		t.Fatal()
	}
}

func TestCPoint_MarshalJSON(t *testing.T) {
	_, pub, _ := GenerateKeyPair()
	bts, err := json.Marshal(pub.CPoint)
	if err != nil {
		t.Fatal(err)
	}
	r2 := CPoint{}
	err = json.Unmarshal(bts, &r2)
	if err != nil {
		t.Fatal(err)
	}
	if !pub.CPoint.Equal(r2) {
		t.Fatal()
	}
}

func TestCPoint_Valid(t *testing.T) {
	_, pub, _ := GenerateKeyPair()
	if !pub.Valid() {
		t.Fatal()
	}
}

func TestGenerateKey(t *testing.T) {
	priv1, pub1, _ := GenerateKeyPair()
	priv2, pub2, _ := GenerateKeyPair()

	if priv1.PrivateKey == nil || priv2.PrivateKey == nil {
		t.Fatal("expected generated private keys")
	}
	if !pub1.Valid() || !pub2.Valid() {
		t.Fatal("expected generated public keys to be valid")
	}
	if pub1.Equal(pub2.CPoint) {
		t.Fatal("expected unique public keys")
	}
}

func TestPublicKey_MarshalJSON(t *testing.T) {
	_, r1, _ := GenerateKeyPair()
	bts, err := json.Marshal(r1)
	if err != nil {
		t.Fatal(err)
	}
	r2 := PublicKey{}
	err = json.Unmarshal(bts, &r2)
	if err != nil {
		t.Fatal(err)
	}
	if !r1.Equal(r2.CPoint) {
		t.Fatal()
	}
}

func TestPublicKey_UnmarshalJSON_Invalid(t *testing.T) {
	var pub PublicKey
	err := json.Unmarshal([]byte(`"not-a-valid-key"`), &pub)
	if err == nil {
		t.Fatal("expected error for invalid public key")
	}
}

func TestPrivateKey_MarshalJSON(t *testing.T) {
	r1, _, _ := GenerateKeyPair()
	bts, err := json.Marshal(r1)
	if err != nil {
		t.Fatal(err)
	}
	r2 := PrivateKey{}
	err = json.Unmarshal(bts, &r2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r1.Bytes(), r2.Bytes()) {
		t.Fatal()
	}
}

func TestPrivateKey_Public(t *testing.T) {
	priv, pub, _ := GenerateKeyPair()
	if !priv.Public().Equal(pub.CPoint) {
		t.Fatal("expected derived public key to match generated public key")
	}
}

func TestSharedSecret_Symmetric(t *testing.T) {
	privA, pubA, _ := GenerateKeyPair()
	privB, pubB, _ := GenerateKeyPair()

	secretAB, err := privA.SharedSecret(pubB)
	if err != nil {
		t.Fatal(err)
	}
	secretBA, err := privB.SharedSecret(pubA)
	if err != nil {
		t.Fatal(err)
	}

	if len(secretAB) == 0 || len(secretBA) == 0 {
		t.Fatal("expected non-empty shared secrets")
	}
	if !bytes.Equal(secretAB, secretBA) {
		t.Fatal("expected shared secrets to match")
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	priv, pub, _ := GenerateKeyPair()

	plaintext := []byte("hello, world")

	ciphertext, err := encrypt(pub, plaintext)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	decrypted, err := decrypt(ciphertext, &priv)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatalf("plaintext mismatch: got %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptProducesDifferentCiphertexts(t *testing.T) {
	_, pub, _ := GenerateKeyPair()
	plaintext := []byte("same message")

	ct1, err := encrypt(pub, plaintext)
	if err != nil {
		t.Fatalf("first encrypt failed: %v", err)
	}
	ct2, err := encrypt(pub, plaintext)
	if err != nil {
		t.Fatalf("second encrypt failed: %v", err)
	}

	// Ephemeral keys mean ciphertexts must differ
	if bytes.Equal(ct1, ct2) {
		t.Fatal("expected different ciphertexts for same plaintext due to ephemeral keys")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	_, pub, _ := GenerateKeyPair()
	wrongPriv, _, _ := GenerateKeyPair()

	ciphertext, err := encrypt(pub, []byte("secret"))
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	_, err = decrypt(ciphertext, &wrongPriv)
	if err == nil {
		t.Fatal("expected decryption to fail with wrong key")
	}
}

func TestDecryptTruncatedInput(t *testing.T) {
	_, pub, _ := GenerateKeyPair()

	ciphertext, err := encrypt(pub, []byte("secret"))
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	for _, truncLen := range []int{0, 1, CPointSize - 1, CPointSize, CPointSize + 1} {
		_, err := decrypt(ciphertext[:truncLen], nil)
		if err == nil {
			t.Errorf("expected error for truncated input of length %d", truncLen)
		}
	}
}

func TestEncryptDecryptEmptyPlaintext(t *testing.T) {
	priv, pub, _ := GenerateKeyPair()

	ciphertext, err := encrypt(pub, []byte{})
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	decrypted, err := decrypt(ciphertext, &priv)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	if !bytes.Equal([]byte{}, decrypted) && decrypted != nil {
		t.Fatalf("expected empty plaintext, got %q", decrypted)
	}
}
