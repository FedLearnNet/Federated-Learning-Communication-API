package util

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"testing"
)

func mustParseCertPEM(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("certificate is not PEM encoded")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	return cert
}

func mustSignTestCSR(t *testing.T, ca *RunCA, commonName string) (keyPEM []byte, certPEM []byte) {
	t.Helper()
	keyPEM, csrPEM, err := GenerateClientCSR(commonName)
	if err != nil {
		t.Fatalf("failed to create CSR: %v", err)
	}
	csr, _, err := ParseClientCSR(csrPEM, commonName)
	if err != nil {
		t.Fatalf("failed to parse CSR: %v", err)
	}
	certPEM, err = ca.SignClientCSR(csr)
	if err != nil {
		t.Fatalf("failed to sign CSR: %v", err)
	}
	return keyPEM, certPEM
}

func TestRunCA_SignAndVerifyRoundTrip(t *testing.T) {
	ca, err := GenerateRunCA()
	if err != nil {
		t.Fatalf("failed to create CA: %v", err)
	}
	keyPEM, certPEM := mustSignTestCSR(t, ca, "client-a")

	// the certificate has to belong to the private key generated with the CSR
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		t.Fatalf("certificate does not match private key: %v", err)
	}

	cert := mustParseCertPEM(t, certPEM)
	if err := ca.VerifyClientCert(cert, "client-a"); err != nil {
		t.Fatalf("expected certificate to verify, got: %v", err)
	}
	if cert.IsCA {
		t.Error("client certificate must not be a CA")
	}
	if cert.NotAfter.After(ca.Cert.NotAfter) {
		t.Error("client certificate must not outlive its CA")
	}
}

func TestRunCA_VerifyRejectsOtherClient(t *testing.T) {
	ca, err := GenerateRunCA()
	if err != nil {
		t.Fatal(err)
	}
	_, certPEM := mustSignTestCSR(t, ca, "client-a")
	if err := ca.VerifyClientCert(mustParseCertPEM(t, certPEM), "client-b"); err == nil {
		t.Fatal("expected certificate of another client to be rejected")
	}
}

func TestRunCA_VerifyRejectsCertOfOtherCA(t *testing.T) {
	ca, err := GenerateRunCA()
	if err != nil {
		t.Fatal(err)
	}
	otherCA, err := GenerateRunCA()
	if err != nil {
		t.Fatal(err)
	}
	_, certPEM := mustSignTestCSR(t, otherCA, "client-a")
	if err := ca.VerifyClientCert(mustParseCertPEM(t, certPEM), "client-a"); err == nil {
		t.Fatal("expected certificate signed by another CA to be rejected")
	}
	if err := ca.VerifyClientCert(nil, "client-a"); err == nil {
		t.Fatal("expected missing certificate to be rejected")
	}
}

// A client certificate must not be usable to sign further certificates.
func TestRunCA_VerifyRejectsCertSignedByClientCert(t *testing.T) {
	ca, err := GenerateRunCA()
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, certPEM := mustSignTestCSR(t, ca, "client-a")
	clientCert := mustParseCertPEM(t, certPEM)
	keyBlock, _ := pem.Decode(keyPEM)
	clientKey, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	forgedKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := randomSerial()
	forgedDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "client-b"},
		NotBefore:    clientCert.NotBefore,
		NotAfter:     clientCert.NotAfter,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, clientCert, &forgedKey.PublicKey, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := x509.ParseCertificate(forgedDER)
	if err != nil {
		t.Fatal(err)
	}
	if err := ca.VerifyClientCert(forged, "client-b"); err == nil {
		t.Fatal("expected certificate signed by a client certificate to be rejected")
	}
}

func TestParseClientCSR_Rejects(t *testing.T) {
	_, csrPEM, err := GenerateClientCSR("client-a")
	if err != nil {
		t.Fatal(err)
	}

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	rsaCSRDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "client-a"},
	}, rsaKey)
	if err != nil {
		t.Fatal(err)
	}

	// flip a bit of the signature (last bytes of the DER encoding)
	block, _ := pem.Decode(csrPEM)
	tampered := append([]byte(nil), block.Bytes...)
	tampered[len(tampered)-1] ^= 0x01

	cases := map[string]struct {
		csr []byte
		cn  string
	}{
		"not PEM":           {[]byte("not a csr"), "client-a"},
		"wrong PEM type":    {pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes}), "client-a"},
		"wrong common name": {csrPEM, "client-b"},
		"RSA key":           {pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: rsaCSRDER}), "client-a"},
		"bad signature":     {pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: tampered}), "client-a"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseClientCSR(tc.csr, tc.cn); !errors.Is(err, ErrInvalidCSR) {
				t.Fatalf("expected ErrInvalidCSR, got %v", err)
			}
		})
	}
}

// Whatever a CSR requests besides key and common name must not end up in the certificate.
func TestSignClientCSR_IgnoresRequestedExtras(t *testing.T) {
	ca, err := GenerateRunCA()
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:  pkix.Name{CommonName: "client-a", Organization: []string{"Evil Corp"}},
		DNSNames: []string{"relay.example.com"},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	csr, _, err := ParseClientCSR(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}), "client-a")
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := ca.SignClientCSR(csr)
	if err != nil {
		t.Fatal(err)
	}
	cert := mustParseCertPEM(t, certPEM)
	if len(cert.DNSNames) != 0 || len(cert.Subject.Organization) != 0 {
		t.Errorf("expected requested extras to be dropped, got DNS names %v and organization %v", cert.DNSNames, cert.Subject.Organization)
	}
	if len(cert.ExtKeyUsage) != 1 || cert.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth {
		t.Errorf("expected only client auth usage, got %v", cert.ExtKeyUsage)
	}
}
