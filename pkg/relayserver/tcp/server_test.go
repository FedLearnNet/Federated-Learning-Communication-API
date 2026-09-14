package tcp

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestNewTCPServer_ProdRequiresValidTLS(t *testing.T) {
	if _, err := NewTCPServer(nil, "prod", "off", nil, nil, nil, nil, nil); err == nil {
		t.Fatal("expected production mode to require TLS")
	}
	if _, err := NewTCPServer(nil, "prod", "on", nil, nil, nil, nil, nil); err == nil {
		t.Fatal("expected production mode to reject missing certs")
	}
}

func TestValidateCertificateChain_RejectsExpiredCertificate(t *testing.T) {
	certDER, _, err := makeTestRootSignedCert(time.Now().Add(-time.Hour), time.Now().Add(-time.Minute), "example.com")
	if err != nil {
		t.Fatalf("failed creating cert: %v", err)
	}

	if err := validateCertificateChain([][]byte{certDER}, "prod", nil); err == nil {
		t.Fatal("expected expired certificate to be rejected")
	}
}

func TestValidateCertificateChain_AcceptsTrustedDomainCertificate(t *testing.T) {
	certDER, caDER, err := makeTestRootSignedCert(time.Now().Add(-time.Hour), time.Now().Add(time.Hour), "example.com")
	if err != nil {
		t.Fatalf("failed creating cert: %v", err)
	}

	roots := x509.NewCertPool()
	roots.AddCert(mustParseCert(t, caDER))
	domain := "example.com"
	if err := validateCertificateChainWithRoots([][]byte{certDER}, roots, "prod", &domain); err != nil {
		t.Fatalf("expected trusted cert for provided domain but got: %v", err)
	}
}

func TestValidateCertificateChain_RejectsUntrustedCertInProd(t *testing.T) {
	certDER, _, err := makeTestSelfSignedCert(time.Now().Add(-time.Hour), time.Now().Add(time.Hour), "example.com")
	if err != nil {
		t.Fatalf("failed creating cert: %v", err)
	}

	if err := validateCertificateChain([][]byte{certDER}, "prod", nil); err == nil || !strings.Contains(err.Error(), "trusted CA") {
		t.Fatal("expected untrusted certificate to fail in prod mode")
	}
}

func mustParseCert(t *testing.T, certDER []byte) *x509.Certificate {
	t.Helper()
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	return cert
}

func makeTestSelfSignedCert(notBefore, notAfter time.Time, dnsName string) ([]byte, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, nil, err
	}

	certTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: dnsName,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{dnsName},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, certTemplate, certTemplate, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certDER, keyPEM, nil
}

func makeTestRootSignedCert(notBefore, notAfter time.Time, dnsName string) ([]byte, []byte, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Root CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour * 24),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, err
	}

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	leafTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: dnsName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{dnsName},
		BasicConstraintsValid: true,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	return leafDER, caDER, nil
}
