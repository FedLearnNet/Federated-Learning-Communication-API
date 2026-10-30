package util

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

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

func TestCheckApiKey(t *testing.T) {
	k1, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}

	if !k1.CheckApiKey(k1) {
		t.Error("expected identical keys to match")
	}
	if k1.CheckApiKey(k2) {
		t.Error("expected different keys not to match")
	}
	if k1.CheckApiKey(k1[:len(k1)-1]) {
		t.Error("expected keys of different length not to match")
	}
	if k1.CheckApiKey("") || APIKey("").CheckApiKey(k1) {
		t.Error("expected empty key not to match")
	}
}

func TestAPIKey_RedactedInFmtButNotInJSON(t *testing.T) {
	k, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}

	formatted := fmt.Sprintf("%v|%+v|%s|%q|%x|%#v", k, struct{ Key APIKey }{k}, k, k, k, k)
	if strings.Contains(formatted, string(k)) {
		t.Fatalf("expected key to be redacted in fmt output, got %q", formatted)
	}
	if !strings.Contains(formatted, "[REDACTED]") {
		t.Fatalf("expected redaction marker in fmt output, got %q", formatted)
	}

	encoded, err := json.Marshal(k)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `"`+string(k)+`"` {
		t.Fatalf("expected JSON to contain the raw key, got %s", encoded)
	}
	if k.String() != string(k) {
		t.Fatal("expected String() to return the raw key")
	}
}
