package util

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// RunCertValidity is how long a per-run CA and the client certificates it signs are valid.
// FL runs only live in the memory of the relay server, so this is just an upper bound.
const RunCertValidity = 90 * 24 * time.Hour

// ErrInvalidCSR is returned when a certificate signing request cannot be accepted.
var ErrInvalidCSR = errors.New("invalid certificate signing request")

// RunCA is the certificate authority of one FL run. The relay server creates one per run and
// uses it to sign the client certificates the controllers present on the TCP connection (mTLS).
type RunCA struct {
	Cert *x509.Certificate
	Pool *x509.CertPool
	// unexported so the private key is never printed when a struct holding the CA is logged
	key *ecdsa.PrivateKey
}

func randomSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// GenerateRunCA creates a new self-signed CA that may only sign leaf certificates.
func GenerateRunCA() (*RunCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("CA private key cannot be created: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, fmt.Errorf("CA serial number cannot be generated: %w", err)
	}

	tml := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "FL-Net FL run CA",
			Organization: []string{"FL-Net Project"},
		},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(RunCertValidity),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tml, tml, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("CA certificate cannot be created: %w", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("CA certificate cannot be parsed: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &RunCA{Cert: cert, Pool: pool, key: key}, nil
}

// GenerateClientCSR creates a new private key and a certificate signing request for it.
// Both are returned PEM encoded. The private key must never leave the process.
func GenerateClientCSR(commonName string) (keyPEM []byte, csrPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("private key cannot be created: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("private key cannot be encoded: %w", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: commonName},
	}, key)
	if err != nil {
		return nil, nil, fmt.Errorf("certificate signing request cannot be created: %w", err)
	}

	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	csrPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	return keyPEM, csrPEM, nil
}

// ParseClientCSR parses and validates a PEM encoded CSR and returns the SHA-256 hash of its
// public key (SubjectPublicKeyInfo). Only ECDSA P-256 keys are accepted and the common name
// must equal expectedCN. All errors wrap ErrInvalidCSR.
func ParseClientCSR(csrPEM []byte, expectedCN string) (*x509.CertificateRequest, [sha256.Size]byte, error) {
	var keyHash [sha256.Size]byte

	block, _ := pem.Decode(csrPEM)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, keyHash, fmt.Errorf("%w: not a PEM encoded certificate request", ErrInvalidCSR)
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, keyHash, fmt.Errorf("%w: %v", ErrInvalidCSR, err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, keyHash, fmt.Errorf("%w: signature check failed: %v", ErrInvalidCSR, err)
	}
	pubKey, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || pubKey.Curve != elliptic.P256() {
		return nil, keyHash, fmt.Errorf("%w: only ECDSA P-256 keys are supported", ErrInvalidCSR)
	}
	if csr.Subject.CommonName != expectedCN {
		return nil, keyHash, fmt.Errorf("%w: common name does not match the requesting client", ErrInvalidCSR)
	}

	keyHash = sha256.Sum256(csr.RawSubjectPublicKeyInfo)
	return csr, keyHash, nil
}

// SignClientCSR signs a CSR validated by ParseClientCSR and returns the PEM encoded client
// certificate. Only the public key and the common name are taken from the CSR, requested
// extensions (e.g. SANs or CA flags) are ignored.
func (ca *RunCA) SignClientCSR(csr *x509.CertificateRequest) ([]byte, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, fmt.Errorf("serial number cannot be generated: %w", err)
	}

	tml := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: csr.Subject.CommonName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              ca.Cert.NotAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tml, ca.Cert, csr.PublicKey, ca.key)
	if err != nil {
		return nil, fmt.Errorf("client certificate cannot be created: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), nil
}

// VerifyClientCert checks that cert is a currently valid client certificate signed by this CA
// and issued for expectedCN.
func (ca *RunCA) VerifyClientCert(cert *x509.Certificate, expectedCN string) error {
	if cert == nil {
		return errors.New("no client certificate presented")
	}
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     ca.Pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return fmt.Errorf("client certificate is not signed by the CA of this FL run: %w", err)
	}
	if cert.Subject.CommonName != expectedCN {
		return errors.New("client certificate was issued for another client")
	}
	return nil
}
