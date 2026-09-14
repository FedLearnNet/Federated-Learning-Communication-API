package util

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// SYMMETRIC API KEY (For client AUTH)
// APIKey stores a generated application/client token as hex text.
type APIKey string

// APIKeyEntropyBytes is the default raw byte size used when generating API keys.
const API_KEY_DEFAULT_ENTROPY_BYTES = 256

// Encode returns the wire format for the key.
func (k APIKey) EncodeAPIKeybytes() []byte {
	return []byte(k)
}

// DecodeAPIKeyBytes validates and constructs an APIKey from its wire format.
func DecodeAPIKeyBytes(b []byte) (APIKey, error) {
	if len(b) == 0 {
		return "", fmt.Errorf("api key must not be empty")
	}
	if _, err := hex.DecodeString(string(b)); err != nil {
		return "", err
	}
	return APIKey(string(b)), nil
}

// Translation from APIKey to string to satisfy type checkers
func (k APIKey) String() string {
	return string(k)
}

// JSON helpers so it's easier to change from string to another type in the future if needed
func (k APIKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(k))
}

func (k *APIKey) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*k = APIKey(s)
	return nil
}

func (provided APIKey) CheckApiKey(expected APIKey) bool {
	// Hash both so they're always the same length,
	// and to avoid leaking the expected key's length
	p := sha256.Sum256([]byte(provided))
	e := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(p[:], e[:]) == 1
}

// GenerateAPIKey creates a new hex-encoded API token.
func GenerateAPIKey() (APIKey, error) {
	if API_KEY_DEFAULT_ENTROPY_BYTES <= 0 {
		return "", fmt.Errorf("API_KEY_DEFAULT_ENTROPY_BYTES must be > 0")
	}
	b := make([]byte, API_KEY_DEFAULT_ENTROPY_BYTES)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return APIKey(hex.EncodeToString(b)), nil
}

// APIKeyEncodedLen returns the number of bytes required to send an API key
// generated from API_KEY_DEFAULT_ENTROPY_BYTES raw entropy bytes.
func APIKeyEncodedLen() int {
	return hex.EncodedLen(API_KEY_DEFAULT_ENTROPY_BYTES)
}
