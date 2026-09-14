package util

import "testing"

func TestGenerateAPIKey_LengthAndUniqueness(t *testing.T) {
	k1, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}

	if k1 == k2 {
		t.Fatal("expected generated API keys to differ")
	}
	// Roundtrip k1 through encode/decode
	if encoded := k1.EncodeAPIKeybytes(); len(encoded) != APIKeyEncodedLen() {
		t.Fatalf("Expected encoded API key length of %d, got %d", APIKeyEncodedLen(), len(encoded))
	}
	decoded, err := DecodeAPIKeyBytes(k1.EncodeAPIKeybytes())
	if err != nil {
		t.Fatal(err)
	}
	if decoded != k1 {
		t.Fatal("expected API key encode/decode roundtrip")
	}
}
