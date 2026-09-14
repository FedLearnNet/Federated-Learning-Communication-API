package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// ASYMMETRIC KEYS (For E2E encryption)
type PublicKey struct {
	CPoint
}

type PrivateKey struct {
	*ecdh.PrivateKey
}

func GenerateKeyPair() (PrivateKey, PublicKey, error) {
	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return PrivateKey{}, PublicKey{}, err
	}

	pub := priv.PublicKey()
	var cp CPoint
	copy(cp[:], pub.Bytes())

	return PrivateKey{priv}, PublicKey{cp}, nil
}

func (p PrivateKey) Public() PublicKey {
	if p.PrivateKey == nil {
		return PublicKey{}
	}
	pub := p.PrivateKey.PublicKey()
	var cp CPoint
	copy(cp[:], pub.Bytes())
	return PublicKey{cp}
}

// A helper method using ECDH to compute a shared secret based on
// - private key of the sender (Ephemeral key regenerated for each message)
// - public key of the recipient (Long-term key)
// ECDH computes the same shared secret for
// - sender:   ephemeral private key e with recipient long-term public key B = bG
// - recipient: long-term private key b with sender ephemeral public key E = eG
// Private key is scalar, public key is scalar multiplication with the curve base point G.
// Shared secret of the sender:    e * (bG) = ebG
// Shared secret of the recipient: b * (eG) = beG = ebG
// G is always the same, see ecdh.X25519() docs for more details
func (p PrivateKey) SharedSecret(pub PublicKey) ([]byte, error) {
	if p.PrivateKey == nil {
		return nil, fmt.Errorf("private key is nil")
	}
	peer, err := curve.NewPublicKey(pub.CPoint[:])
	if err != nil {
		return nil, err
	}
	return p.PrivateKey.ECDH(peer)
}

// Serialization
func (p PrivateKey) MarshalJSON() ([]byte, error) {
	if p.PrivateKey == nil {
		return json.Marshal("")
	}
	s := hex.EncodeToString(p.Bytes())
	return json.Marshal(s)
}

func (p *PrivateKey) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	bts, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	if len(bts) == 0 {
		p.PrivateKey = nil
		return nil
	}
	priv, err := curve.NewPrivateKey(bts)
	if err != nil {
		return err
	}
	p.PrivateKey = priv
	return nil
}

func (cp PublicKey) MarshalJSON() ([]byte, error) {
	s := cp.Hex()
	return json.Marshal(s)
}

func (cp *PublicKey) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := parseCPointFromHexStrict(s)
	if err != nil {
		return err
	}
	if !parsed.Valid() {
		return fmt.Errorf("invalid public key")
	}
	*cp = PublicKey{parsed}
	return nil
}

// CRYPTO HELPER: Public Key via elliptic curve
var curve = ecdh.X25519()

// Public Key: CPoint represents a point on the elliptic curve,
// It is stored as a fixed-size byte array.
const CPointSize = 32

type CPoint [CPointSize]byte

func (cp CPoint) Equal(cp2 CPoint) bool {
	return bytes.Equal(cp[:], cp2[:])
}

func (cp CPoint) Valid() bool {
	_, err := curve.NewPublicKey(cp[:])
	return err == nil
}

func (cp CPoint) Hex() string {
	return hex.EncodeToString(cp[:])
}

func parseCPointFromHexStrict(s string) (CPoint, error) {
	var cp CPoint
	d, err := hex.DecodeString(s)
	if err != nil {
		return cp, err
	}
	// The CPoint is fixed size, enforece this here
	if len(d) != len(cp) {
		return cp, fmt.Errorf("invalid public key length")
	}
	copy(cp[:], d)
	return cp, nil
}

func (cp CPoint) MarshalJSON() ([]byte, error) {
	s := cp.Hex()
	return json.Marshal(s)
}

func (cp *CPoint) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := parseCPointFromHexStrict(s)
	if err != nil {
		return err
	}
	*cp = parsed
	return nil
}

// Encrypt encrypts the given bytes for destPubKey.
// Writes: Ephemeral public key | Nonce | Ciphertext+Tag
func Encrypt(destPubKey PublicKey, plaintext []byte) ([]byte, error) {
	return encrypt(destPubKey, plaintext)
}

// Decrypt decrypts bytes that were encrypted with Encrypt using ownPrivKey.
func Decrypt(bts []byte, ownPrivKey *PrivateKey) ([]byte, error) {
	return decrypt(bts, ownPrivKey)
}

// Encrypts the given bytes
// Generated an ephemeral key pair and
// derives a shared secret using ECDH with the given destination public key
// The decrypting client will derive the same shared secret using its private key
// and the ephemeral public key
// The shared secret is then used as key for AES encryption in GCM mode
// Encryption via symmetric AES is a LOT faster
// Writes: Ephermal public Key | Nonce | Ciphertext+Tag
func encrypt(destPubKey PublicKey, plaintext []byte) ([]byte, error) {
	// Generate the ephemeral key pair for this message
	ephemeralPriv, ephemeralPub, err := GenerateKeyPair()
	if err != nil {
		return nil, err
	}
	if ephemeralPriv.PrivateKey == nil {
		return nil, errors.New("failed to generate ephemeral key pair")
	}

	// Create the shared secret
	sharedSecret, err := ephemeralPriv.SharedSecret(destPubKey)
	if err != nil {
		return nil, err
	}

	// Encrypt the message with AES-GCM using the shared secret as key
	aesCiphertext, err := aesEncrypt(sharedSecret, plaintext)
	if err != nil {
		return nil, err
	}

	// Write Layout: [ ephemeral pubkey (32 bytes) | nonce | ciphertext+tag ]
	var buf bytes.Buffer
	cR := ephemeralPub.CPoint
	buf.Write(cR[:])
	buf.Write(aesCiphertext)
	return buf.Bytes(), nil
}

// Decrypts the given bytes
// Expects the ephemeral public key in front of the ciphertext,
// which is used to derive the same shared secret via ECDH as the encrypting client did
// Then uses the shared secret as key for AES decryption in GCM mode to decrypt the message
// If the ciphertext is too short to contain the ephemeral public key,
// or if the ephemeral public key is invalid, an error is returned
// Reads: Ephermal public Key | Nonce | Ciphertext+Tag
func decrypt(bts []byte, ownPrivKey *PrivateKey) ([]byte, error) {
	if ownPrivKey == nil {
		return nil, errors.New("private key is nil")
	}
	if len(bts) < CPointSize {
		return nil, errors.New("ciphertext too short")
	}

	// Get the ephemeral public key from the beginning of the message
	var cR CPoint
	copy(cR[:], bts[:CPointSize])
	bts = bts[CPointSize:]

	// Generate the shared secret. Will be the same as the encrypting client generated
	sharedSecret, err := ownPrivKey.SharedSecret(PublicKey{CPoint: cR})
	if err != nil {
		return nil, err
	}

	// Decrypt the message with AES-GCM using the shared secret as key
	return aesDecrypt(sharedSecret, bts)
}

// newAESGCM derives an AES-GCM cipher from a raw shared secret.
func newAESGCM(sharedSecret []byte) (cipher.AEAD, error) {
	skey := sha256.Sum256(sharedSecret)
	block, err := aes.NewCipher(skey[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// aesEncrypt encrypts plaintext using AES-GCM with the given shared secret.
// Returns [ nonce | ciphertext+tag ].
func aesEncrypt(sharedSecret, plaintext []byte) ([]byte, error) {
	aesGCM, err := newAESGCM(sharedSecret)
	if err != nil {
		return nil, err
	}
	nonce, err := RandomBytes(uint32(aesGCM.NonceSize()))
	if err != nil {
		return nil, err
	}
	if len(nonce) != aesGCM.NonceSize() {
		return nil, errors.New("failed to generate nonce")
	}
	var buf bytes.Buffer
	buf.Write(nonce)
	buf.Write(aesGCM.Seal(nil, nonce, plaintext, nil))
	return buf.Bytes(), nil
}

// aesDecrypt decrypts AES-GCM ciphertext of the form [ nonce | ciphertext+tag ].
func aesDecrypt(sharedSecret, bts []byte) ([]byte, error) {
	aesGCM, err := newAESGCM(sharedSecret)
	if err != nil {
		return nil, err
	}
	nonceSize := aesGCM.NonceSize()
	if len(bts) < nonceSize+aesGCM.Overhead() {
		return nil, errors.New("ciphertext too short")
	}
	nonce, ciphertext := bts[:nonceSize], bts[nonceSize:]
	return aesGCM.Open(nil, nonce, ciphertext, nil)
}

// MISC HELPER FUNCTIONS
func RandomBytes(l uint32) ([]byte, error) {
	if l <= 0 {
		return []byte{}, fmt.Errorf("Creating random bytes only possible on at least 1 byte, requested length: %d", l)
	}
	b := make([]byte, l)
	if _, err := rand.Read(b); err != nil {
		return []byte{}, err
	}
	return b, nil
}
