package tcp

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	bridge "fc_controller/pkg/relayserver/bridge"
	enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
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

// --- mTLS client registration ---

// startTestTLSRelay starts a relay TCP server with a self-signed certificate on a free port.
// The result of each client registration is sent to the returned channel.
func startTestTLSRelay(t *testing.T, store *bridge.FlRunStore) (string, <-chan error) {
	t.Helper()
	domain := "localhost"
	certDER, keyPEM, err := makeTestSelfSignedCert(time.Now().Add(-time.Hour), time.Now().Add(time.Hour), domain)
	if err != nil {
		t.Fatalf("failed creating cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	tlsConfig, err := buildTLSConfigFromPair(certPEM, keyPEM, "dev", &domain, nil, nil)
	if err != nil {
		t.Fatalf("failed creating TLS config: %v", err)
	}
	s := &RelayServiceTCP{store: store, tlsConfig: tlsConfig, tlsMode: "self-signed"}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	results := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			err = s.handleClientRegistration(conn)
			if err != nil {
				_ = conn.Close()
			}
			results <- err
		}
	}()
	return listener.Addr().String(), results
}

// newTestClientCert returns a TLS certificate for clientID signed by the CA of flRun.
func newTestClientCert(t *testing.T, flRun *bridge.FlRunMeta, clientID shared.ClientID) *tls.Certificate {
	t.Helper()
	keyPEM, csrPEM, err := util.GenerateClientCSR(clientID.ToString())
	if err != nil {
		t.Fatalf("failed to create CSR: %v", err)
	}
	certPEM, err := flRun.SignClientCert(clientID, csrPEM)
	if err != nil {
		t.Fatalf("failed to sign CSR: %v", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("failed to load client certificate: %v", err)
	}
	return &cert
}

// registerTestClient connects as clientID of flRun (with its correct client key) and
// returns the result of the registration on the relay server.
func registerTestClient(t *testing.T, addr string, results <-chan error, flRun *bridge.FlRunMeta, clientID shared.ClientID, cert *tls.Certificate) error {
	t.Helper()
	tlsConfig := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}
	if cert != nil {
		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return cert, nil
		}
	}
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// A rejected client may already see the write fail, only the relay's verdict matters here
	header := shared.NewConnectionSetupHeader(flRun.Channel, clientID, flRun.ClientId2ClientKey[clientID])
	_ = shared.WriteConnectionSetupHeader(shared.NewTCPIO(conn, nil), header)

	select {
	case err := <-results:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("relay did not handle the registration in time")
		return nil
	}
}

func TestHandleClientRegistration_ClientCertificate(t *testing.T) {
	store := bridge.NewFlRunStore()
	t.Cleanup(store.Shutdown)
	flRun, err := store.Create(2, enums.AppVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	otherRun, err := store.Create(2, enums.AppVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	addr, results := startTestTLSRelay(t, store)
	clientA, clientB := flRun.ClientIDOrder[0], flRun.ClientIDOrder[1]

	if err := registerTestClient(t, addr, results, flRun, clientA, nil); err == nil {
		t.Error("expected client without certificate to be rejected")
	}
	if err := registerTestClient(t, addr, results, flRun, clientA, newTestClientCert(t, otherRun, otherRun.ClientIDOrder[0])); err == nil {
		t.Error("expected client with a certificate of another FL run to be rejected")
	}
	// client B knows the client key of A, but only has its own certificate
	if err := registerTestClient(t, addr, results, flRun, clientA, newTestClientCert(t, flRun, clientB)); err == nil {
		t.Error("expected client with a certificate of another client to be rejected")
	}
	if _, connected := flRun.GetConnection(clientA); connected {
		t.Fatal("rejected client must not be registered")
	}

	if err := registerTestClient(t, addr, results, flRun, clientA, newTestClientCert(t, flRun, clientA)); err != nil {
		t.Errorf("expected client with its own certificate to be accepted, got: %v", err)
	}
	if _, connected := flRun.GetConnection(clientA); !connected {
		t.Error("accepted client must be registered")
	}
}
