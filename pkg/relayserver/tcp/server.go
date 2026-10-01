// Package tcp implements the TLS relay TCP server for the global relay server.
// It accepts client connections, authenticates them against the shared FlRunStore,
// and relays messages between participants in a federated learning run.
package tcp

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	bridge "fc_controller/pkg/relayserver/bridge"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
)

// isStoppedErr returns true when err signals a deliberate shutdown rather than
// a real connection failure. Relay goroutines return nil on stopped errors to
// avoid spurious error logs.
func isStoppedErr(err error) bool {
	return errors.Is(err, shared.ErrStopped)
}

// logConnErr logs a connection error at the appropriate level:
// ErrStopped (deliberate run shutdown) → Warn; all other errors → Error.
func logConnErr(msg string, err error) {
	if isStoppedErr(err) {
		logger.Warn(GLOBALTCP, "", "%s: %v", msg, err)
	} else {
		logger.Error(GLOBALTCP, "", "%s: %v", msg, err)
	}
}

const GLOBALTCP = "GLOBAL_TCP"

// RelayServiceTCP is the TLS relay TCP server.
// It reads the shared FlRunStore (owned by the caller) to authenticate clients
// and route messages. It owns no FL run state itself.
type RelayServiceTCP struct {
	store     *bridge.FlRunStore
	listener  net.Listener
	tlsConfig *tls.Config
	tlsMode   string // "on", "self-signed", or "off"
}

// NewTCPServer creates a new TCP relay server backed by the given store.
// Mode dependent:
// tlsMode -> on
//
//	Reads the given certificate and private key files.
//
// tlsMode -> self-signed
//
//	Generates a new certificate and private key.
//
// tlsMode -> off
//
//	Does not use TLS. Warning: this is insecure and should only be used for testing.
//
// If Mode is prod, this enforces the use of TLS with CA authority signed certificates.
func NewTCPServer(store *bridge.FlRunStore, mode string, tlsMode string, tlsPublicCertPath *string, tlsPrivateKeyPath *string, tlsDomain *string, minVersion *uint16, maxVersion *uint16) (*RelayServiceTCP, error) {
	if mode == "prod" && tlsMode != "on" {
		return nil, errors.New("TLS must be enabled with a valid certificate chain in production mode")
	}

	tlsConfig, err := buildTLSConfig(mode, tlsMode, tlsPublicCertPath, tlsPrivateKeyPath, tlsDomain, minVersion, maxVersion)
	if err != nil {
		return nil, err
	}
	if tlsMode == "off" {
		return &RelayServiceTCP{
			store:     store,
			tlsConfig: nil,
			tlsMode:   tlsMode,
		}, nil
	}
	if tlsConfig == nil {
		return nil, errors.New("could not create TLS listener")
	}

	return &RelayServiceTCP{
		store:     store,
		tlsConfig: tlsConfig,
		tlsMode:   tlsMode,
	}, nil
}

func buildTLSConfig(mode string, tlsMode string, tlsPublicCertPath *string, tlsPrivateKeyPath *string, tlsDomain *string, minVersion *uint16, maxVersion *uint16) (*tls.Config, error) {
	switch tlsMode {
	case "off":
		logger.Warn(GLOBALTCP, "", "TLS is disabled. This is insecure and should only be used for testing.")
		return nil, nil
	case "on":
		return loadProvidedTLSConfig(mode, tlsPublicCertPath, tlsPrivateKeyPath, tlsDomain, minVersion, maxVersion)
	case "self-signed":
		logger.Warn(GLOBALTCP, "", "TLS is enabled with self-signed certificate. This is insecure and should only be used for testing.")
		return generateSelfSignedTLSConfig(mode, tlsPublicCertPath, tlsPrivateKeyPath, tlsDomain, minVersion, maxVersion)
	default:
		return nil, fmt.Errorf("unsupported TLS mode %q", tlsMode)
	}
}

// resolveCertFiles returns the certificate and private key paths.
// Check the file content length to determine if the files exist and are non-empty.
func resolveCertFiles(tlsPublicCertPath *string, tlsPrivateKeyPath *string, allowDefault bool) (string, string, error) {
	if !allowDefault {
		if tlsPublicCertPath == nil || strings.TrimSpace(*tlsPublicCertPath) == "" {
			return "", "", errors.New("TLS mode requires an explicit certificate path")
		}
		if tlsPrivateKeyPath == nil || strings.TrimSpace(*tlsPrivateKeyPath) == "" {
			return "", "", errors.New("TLS mode requires an explicit private key path")
		}
	}

	certFile := "relay_cert.pem"
	if tlsPublicCertPath != nil && strings.TrimSpace(*tlsPublicCertPath) != "" {
		certFile = strings.TrimSpace(*tlsPublicCertPath)
		if _, err := os.Stat(certFile); err != nil {
			return "", "", fmt.Errorf("configured certificate path %q does not exist: %w", certFile, err)
		}
	}

	privateKeyFile := "relay_key.pem"
	if tlsPrivateKeyPath != nil && strings.TrimSpace(*tlsPrivateKeyPath) != "" {
		privateKeyFile = strings.TrimSpace(*tlsPrivateKeyPath)
		if _, err := os.Stat(privateKeyFile); err != nil {
			return "", "", fmt.Errorf("configured private key path %q does not exist: %w", privateKeyFile, err)
		}
	}
	return certFile, privateKeyFile, nil
}

func buildTLSConfigFromPair(certPEM []byte, keyPEM []byte, mode string, tlsDomain *string, minVersion *uint16, maxVersion *uint16) (*tls.Config, error) {
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("error loading certificate/key pair: %w", err)
	}

	if err := validateCertificateChain(tlsCert.Certificate, mode, tlsDomain); err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		Rand:         rand.Reader,
		Certificates: []tls.Certificate{tlsCert},
		// mTLS: each FL run has its own CA and the run is only known once the connection
		// setup header was read, so the handshake just demands a certificate (and proof of
		// its private key). handleClientRegistration verifies it against the run's CA.
		ClientAuth: tls.RequireAnyClientCert,
	}
	if minVersion != nil {
		tlsConfig.MinVersion = *minVersion
	}
	if maxVersion != nil {
		tlsConfig.MaxVersion = *maxVersion
	}

	logger.Info(GLOBALTCP, "", "TLS config created successfully")
	return tlsConfig, nil
}

func loadProvidedTLSConfig(mode string, tlsPublicCertPath *string, tlsPrivateKeyPath *string, tlsDomain *string, minVersion *uint16, maxVersion *uint16) (*tls.Config, error) {
	certFile, privateKeyFile, err := resolveCertFiles(tlsPublicCertPath, tlsPrivateKeyPath, false)
	if err != nil {
		return nil, err
	}
	logger.Info(GLOBALTCP, "", "Loading TLS certificate from %s and key from %s", certFile, privateKeyFile)

	relayCert, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("could not read certificate file %s: %w", certFile, err)
	}
	relayKey, err := os.ReadFile(privateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("could not read private key file %s: %w", privateKeyFile, err)
	}

	return buildTLSConfigFromPair(relayCert, relayKey, mode, tlsDomain, minVersion, maxVersion)
}

func generateSelfSignedTLSConfig(mode string, tlsPublicCertPath *string, tlsPrivateKeyPath *string, tlsDomain *string, minVersion *uint16, maxVersion *uint16) (*tls.Config, error) {
	certFile, privateKeyFile, err := resolveCertFiles(tlsPublicCertPath, tlsPrivateKeyPath, true)
	if err != nil {
		return nil, err
	}
	var relayCert []byte
	var relayKey []byte

	if existingCert, err := os.ReadFile(certFile); err == nil && len(existingCert) > 0 {
		relayCert = existingCert
	}
	if existingKey, err := os.ReadFile(privateKeyFile); err == nil && len(existingKey) > 0 {
		relayKey = existingKey
	}

	if len(relayCert) == 0 || len(relayKey) == 0 {
		logger.Info(GLOBALTCP, "", "Generating new self-signed certificate and private key...")

		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("private key cannot be created: %w", err)
		}

		relayKey, err = x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("private key cannot be encoded: %w", err)
		}
		relayKey = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: relayKey})

		serial, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			return nil, fmt.Errorf("serial number cannot be generated: %w", err)
		}

		commonName := "FLNetRelay"
		dnsNames := make([]string, 0, 2)
		if tlsDomain != nil && strings.TrimSpace(*tlsDomain) != "" {
			for _, name := range strings.Split(*tlsDomain, ",") {
				trimmed := strings.TrimSpace(name)
				if trimmed != "" {
					dnsNames = append(dnsNames, trimmed)
				}
			}
			if len(dnsNames) > 0 {
				commonName = dnsNames[0]
			}
		}
		if len(dnsNames) == 0 {
			return nil, errors.New("no valid domain names provided for self-signed certificate.")
		}

		tml := &x509.Certificate{
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().AddDate(5, 0, 0),
			SerialNumber: serial,
			Subject: pkix.Name{
				CommonName:   commonName,
				Organization: []string{"FL-Net Project"},
			},
			DNSNames:              dnsNames,
			KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
		}

		certDER, err := x509.CreateCertificate(rand.Reader, tml, tml, &key.PublicKey, key)
		if err != nil {
			return nil, fmt.Errorf("certificate cannot be created: %w", err)
		}

		relayCert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
		if err := os.WriteFile(certFile, relayCert, 0o600); err != nil {
			logger.Error(GLOBALTCP, "", "Error writing certificate file: %v", err)
		}
		if err := os.WriteFile(privateKeyFile, relayKey, 0o600); err != nil {
			logger.Error(GLOBALTCP, "", "Error writing private key file: %v", err)
		}
		logger.Info(GLOBALTCP, "", "Certificate files written successfully")
		logger.Info(GLOBALTCP, "", "Generated self-signed certificate for %v", dnsNames)
		asciiOverview(tml)
	} // else expect the cert files to exist

	if len(relayCert) == 0 || len(relayKey) == 0 {
		return nil, errors.New("self-signed certificate or key could not be loaded or generated")
	}

	return buildTLSConfigFromPair(relayCert, relayKey, mode, tlsDomain, minVersion, maxVersion)
}

func validateCertificateChain(rawCerts [][]byte, mode string, tlsDomain *string) error {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	return validateCertificateChainWithRoots(rawCerts, roots, mode, tlsDomain)
}

func validateCertificateChainWithRoots(rawCerts [][]byte, roots *x509.CertPool, mode string, tlsDomain *string) error {
	if len(rawCerts) == 0 {
		return errors.New("certificate chain is empty")
	}

	leafCert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("certificate could not be parsed: %w", err)
	}
	if time.Now().Before(leafCert.NotBefore) || time.Now().After(leafCert.NotAfter) {
		return errors.New("certificate is not currently valid")
	}

	intermediates := x509.NewCertPool()
	for _, certDER := range rawCerts[1:] {
		cert, err := x509.ParseCertificate(certDER)
		if err != nil {
			continue
		}
		intermediates.AddCert(cert)
	}

	dnsName := ""
	if tlsDomain != nil && strings.TrimSpace(*tlsDomain) != "" {
		dnsName = strings.TrimSpace(*tlsDomain)
	} else if len(leafCert.DNSNames) > 0 {
		dnsName = leafCert.DNSNames[0]
	} else if leafCert.Subject.CommonName != "" {
		dnsName = leafCert.Subject.CommonName
	}

	verifyOpts := x509.VerifyOptions{
		DNSName:       dnsName,
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   time.Now(),
	}
	if _, err := leafCert.Verify(verifyOpts); err != nil {
		if mode == "prod" {
			return fmt.Errorf("certificate is not signed by a trusted CA or is invalid for %q: %w", dnsName, err)
		}
		logger.Warn(GLOBALTCP, "", "Certificate is not signed by a trusted CA for %q; continuing because non-production mode: %v", dnsName, err)
	}
	return nil
}

// StartServer starts listening on the given port and accepts TLS connections.
// If tlsMode is "off", accepts unencrypted TCP connections.
// Blocks until Shutdown() is called (which closes the listener).
func (s *RelayServiceTCP) StartServer(port int) error {
	var err error
	if s.tlsMode == "off" {
		s.listener, err = net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
		logger.Warn(GLOBALTCP, "", "Starting server in unencrypted mode on port %d. This is insecure and should only be used for testing.", port)
	} else {
		if s.tlsConfig == nil {
			return errors.New("tls config is nil, refusing to start unencrypted tls server")
		}
		s.listener, err = tls.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port), s.tlsConfig)
	}
	if err != nil {
		return err
	}

	defer func() {
		_ = s.listener.Close()
	}()

	if s.tlsMode != "off" {
		logger.Info(GLOBALTCP, "", "Listening on port %d (with TLS)", port)
	} else {
		logger.Info(GLOBALTCP, "", "Listening on port %d (unencrypted)", port)
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// If the error is because Shutdown() called listener.Close(), exit cleanly
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			// For other errors (e.g., EMFILE resource exhaustion),
			// log it and back off slightly to let the OS recover.
			logger.Error(GLOBALTCP, "", "Accept error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		go func(c net.Conn) {
			if err := s.handleClientRegistration(c); err != nil {
				logger.Error(GLOBALTCP, "", "Error handling client registration: %v", err)
				_ = c.Close()
			}
		}(conn)
	}
}

// Shutdown closes the listener, stopping ListenAndAccept.
// To also stop all active FL runs, call store.Shutdown() separately.
func (s *RelayServiceTCP) Shutdown() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
}

// verifyClientCert checks that the TLS client certificate of conn was signed by the CA of
// the FL run for exactly this client. Must be called after the TLS handshake completed,
// e.g. after the first read. Without TLS there is no certificate and nothing is checked.
func (s *RelayServiceTCP) verifyClientCert(conn net.Conn, flRun *bridge.FlRunMeta, clientID shared.ClientID) error {
	if s.tlsMode == "off" {
		return nil
	}
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return errors.New("connection is not a TLS connection")
	}
	peerCerts := tlsConn.ConnectionState().PeerCertificates
	if len(peerCerts) == 0 {
		return errors.New("no client certificate presented")
	}
	return flRun.CA.VerifyClientCert(peerCerts[0], clientID.ToString())
}

// handleClientRegistration reads the connection setup header, authenticates the
// client against the matching FL run (TLS client certificate and client key), responds with the relay key for mutual auth,
// registers the connection, and starts a relay goroutine.
// Does NOT close the connection on failure.
func (s *RelayServiceTCP) handleClientRegistration(conn net.Conn) error {
	// Temporary deadline to avoid hanging on authentication
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	// Create a temporary TCPIO for the authentication handshake (without state,
	// since we don't yet know which run this connection belongs to).
	tempTcpConn := shared.NewTCPIO(conn, nil)
	logger.Debug(GLOBALTCP, "", "New client connection from %s", conn.RemoteAddr().String())

	var authHeader *shared.ConnectionSetupHeader
	err := tempTcpConn.WithHeaderReadLock(func(io shared.TCPIOInterface) error {
		var readErr error
		authHeader, readErr = shared.ReadConnectionSetupHeader(io)
		return readErr
	})
	if err != nil {
		logger.Error(GLOBALTCP, "", "Error reading connection setup header: %v", err)
		return errors.New("Failed to read connection setup header")
	}

	logger.Debug(GLOBALTCP, "", "Succesfully read connection setup header: %+v", authHeader)

	flRun, exists := s.store.Get(authHeader.Channel)
	if !exists {
		logger.Error(GLOBALTCP, "", "AUTH ERROR: No FL run found for channel %s", authHeader.Channel.ToString())
		return errors.New("no FL run found for channel")
	}
	if err := s.verifyClientCert(conn, flRun, authHeader.ClientID); err != nil {
		logger.Error(GLOBALTCP, "", "AUTH ERROR: Client %s has no valid client certificate: %v", authHeader.ClientID.ToString(), err)
		return errors.New("client certificate not valid")
	}
	if !flRun.Authenticate(authHeader.ClientID, authHeader.ClientKey) {
		logger.Error(GLOBALTCP, "", "AUTH ERROR: Client %s not authenticated", authHeader.ClientID.ToString())
		return errors.New("client not authenticated")
	}

	logger.Debug(GLOBALTCP, "", "Successfully authenticated client")

	// Mutual auth: send relay key so the client can verify our identity.
	if err := tempTcpConn.WithHeaderWriteLock(func(io shared.TCPIOInterface) error {
		return shared.WriteConnectionSetupResponse(io, flRun.RelayKey)
	}); err != nil {
		logger.Error(GLOBALTCP, "", "Error sending relay key to client %s: %v", authHeader.ClientID.ToString(), err)
		return errors.New("failed to send relay key response")
	}

	logger.Debug(GLOBALTCP, "", "Successfully send Client our application layer api key for mutual auth")

	// Client is authenticated, remove the temporary deadline and proceed with normal operation.
	_ = conn.SetDeadline(time.Time{}) // Clear the deadline

	// Now that we know the flRun, create a new TCPIO with the run's shared state.
	// This allows the connection to coordinate with other components when the run is stopped.
	tcpConn := shared.NewTCPIO(conn, &flRun.State)

	clientId := authHeader.ClientID
	if err := flRun.AddConnection(clientId, tcpConn); err != nil {
		logger.Error(GLOBALTCP, "", "Error adding connection: %v", err)
		return errors.New("failed to add connection")
	}

	if flRun.IsCoordinator(clientId) {
		logger.Info(GLOBALTCP, "", "Coordinator (ID: %s) connected for channel %s", clientId.ToString(), authHeader.Channel.ToString())
	} else {
		logger.Info(GLOBALTCP, "", "Client (ID: %s) connected for channel %s", clientId.ToString(), authHeader.Channel.ToString())
	}

	go func() {
		if err := s.relayMessages(clientId, flRun); err != nil {
			logger.Error(GLOBALTCP, "", "Error relaying messages for client %s: %v", clientId.ToString(), err)
		} else {
			logger.Info(GLOBALTCP, "", "Stopped relaying messages for client %s", clientId.ToString())
		}
	}()

	return nil
}

// relayMessages reads messages from the client's connection and dispatches them.
// This is the only goroutine ever reading that connection.
// It exits cleanly (nil) when the run is stopped via FlRunMeta.Stop().
func (s *RelayServiceTCP) relayMessages(clientId shared.ClientID, flRun *bridge.FlRunMeta) error {
	isCoordinator := flRun.IsCoordinator(clientId)
	localServerConn, exists := flRun.Connections[clientId]
	if !exists {
		return errors.New("Client connection not found")
	}
	if isCoordinator {
		logger.Info(GLOBALTCP, "", "Starting to relay messages for Coordinator (ID: %s)", clientId.ToString())
	} else {
		logger.Info(GLOBALTCP, "", "Starting to relay messages for Client (ID: %s)", clientId.ToString())
	}

	for {
		// Check if the run is still active. If not, exit cleanly.
		if !flRun.IsActive() {
			logger.Info(GLOBALTCP, "", "Run no longer active; exiting message relay for client %s", clientId.ToString())
			return nil
		}

		unlockRead, err := localServerConn.LockHeaderRead()
		if err != nil {
			if isStoppedErr(err) {
				return nil
			}
			return err
		}

		var cmd byte
		var handlerErr error
		cmd, err = localServerConn.ReadByte()
		if err != nil {
			unlockRead()
			if isStoppedErr(err) {
				return nil
			}
			return err
		}
		logger.Debug(GLOBALTCP, "", "Received command byte %d from client %s", cmd, clientId.ToString())

		switch cmd {
		case shared.CMD_SEND_PUBLIC_KEY:
			var header *shared.PublicKeyAnnouncementHeader
			header, err = shared.ReadPublicKeyAnnouncementHeader(localServerConn, shared.CMD_SEND_PUBLIC_KEY)
			unlockRead()
			if err != nil {
				if isStoppedErr(err) {
					return nil
				}
				return err
			}
			handlerErr = s.processPublicKeyAnnouncement(clientId, flRun, header)
		case shared.CMD_SEND_P2P_DATA, shared.CMD_SEND_SMPC_DATA, shared.CMD_SEND_SMPC_AGG:
			handlerErr = s.handleRelayP2PDataMessage(cmd, clientId, flRun, localServerConn)
			unlockRead()
		case shared.CMD_SEND_PLAIN_DATA:
			if isCoordinator {
				var header *shared.DataPacketHeader
				var payload []byte
				header, payload, err = s.readBroadcastPacketLocked(cmd, clientId, flRun, localServerConn)
				unlockRead()
				if err != nil {
					handlerErr = err
				} else {
					handlerErr = s.broadcastDataPayload(flRun, header, payload)
				}
			} else {
				handlerErr = s.handleRelayToCoordinatorMessage(cmd, clientId, flRun, localServerConn)
				unlockRead()
			}
		case shared.CMD_SEND_PUBLIC_KEYS:
			unlockRead()
			logger.Error(GLOBALTCP, "", "Unsupported command byte %d from client %s", cmd, clientId.ToString())
			return errors.New("Unsupported command byte")
		default:
			unlockRead()
			logger.Error(GLOBALTCP, "", "Unknown command byte %d from client %s", cmd, clientId.ToString())
			return errors.New("Unknown command byte")
		}

		if handlerErr != nil {
			logConnErr(fmt.Sprintf("error handling command %d from client %s", cmd, clientId.ToString()), handlerErr)
		}
		logger.Debug(GLOBALTCP, "", "Handled message without any errors")
	}
}

func (s *RelayServiceTCP) processPublicKeyAnnouncement(clientId shared.ClientID, flRun *bridge.FlRunMeta, header *shared.PublicKeyAnnouncementHeader) error {
	if header.Channel != flRun.Channel {
		logger.Warn(GLOBALTCP, "", "Public-key announcement channel mismatch from %s", clientId.ToString())
	}

	if err := flRun.SetPublicKey(clientId, header.PublicKey); err != nil {
		return fmt.Errorf("failed to store public key for client %s: %w", clientId.ToString(), err)
	}

	logger.Info(GLOBALTCP, "", "Stored public key for client %s", clientId.ToString())
	if err := s.broadcastPublicKeyUpdate(flRun); err != nil {
		return fmt.Errorf("failed to broadcast public-key update: %w", err)
	}
	return nil
}

// readBroadcastPacketLocked expects the caller to hold fromConn's read lock.
func (s *RelayServiceTCP) readBroadcastPacketLocked(cmd byte, clientId shared.ClientID, flRun *bridge.FlRunMeta, fromConn *shared.TCPIO) (*shared.DataPacketHeader, []byte, error) {
	header, err := shared.ReadDataHeader(fromConn, cmd)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read broadcast header: %w", err)
	}

	if !flRun.IsCoordinator(clientId) {
		if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
			return nil, nil, fmt.Errorf("non-coordinator attempted broadcast and failed to discard payload: %w", drainErr)
		}
		return nil, nil, fmt.Errorf("non-coordinator %s attempted broadcast", clientId.ToString())
	}

	if header.FromClientID != clientId {
		if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
			return nil, nil, fmt.Errorf("invalid fromClientID and failed to discard payload: %w", drainErr)
		}
		return nil, nil, fmt.Errorf("discarding broadcast with invalid fromClientID %s for connected client %s", header.FromClientID.ToString(), clientId.ToString())
	}

	if header.DestinationClientID != nil && *header.DestinationClientID != shared.ZERO_CLIENT_ID {
		if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
			return nil, nil, fmt.Errorf("non-zero destination and failed to discard payload: %w", drainErr)
		}
		return nil, nil, fmt.Errorf("discarding broadcast with non-zero destination %s", header.DestinationClientID.ToString())
	}

	payload := make([]byte, header.ContentLength)
	if err := fromConn.ReadBytes(payload); err != nil {
		return nil, nil, fmt.Errorf("failed to read broadcast payload: %w", err)
	}
	return header, payload, nil
}

// ---------------------------------------------------------------------------
// Sending methods — write data to one or more client connections.
// ---------------------------------------------------------------------------

// broadcastDataPayload sends a prepared payload to all non-coordinator recipients in parallel.
func (s *RelayServiceTCP) broadcastDataPayload(flRun *bridge.FlRunMeta, header *shared.DataPacketHeader, payload []byte) error {
	flRun.MutexConnections.RLock()
	targets := make([]*shared.TCPIO, 0, len(flRun.Connections))
	for id, conn := range flRun.Connections {
		if id != header.FromClientID && id != flRun.CoordinatorID {
			targets = append(targets, conn)
		}
	}
	flRun.MutexConnections.RUnlock()

	if len(targets) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	for _, conn := range targets {
		wg.Add(1)
		go func(connInner *shared.TCPIO) {
			defer wg.Done()
			err := connInner.WithHeaderWriteLock(func(io shared.TCPIOInterface) error {
				if err := shared.WriteHeader(io, header); err != nil {
					return err
				}
				if err := shared.WriteContentLength(io, uint64(len(payload))); err != nil {
					return err
				}
				return io.WriteBytes(payload)
			})
			if err != nil {
				logConnErr("failed to broadcast data payload", err)
			}
		}(conn)
	}
	wg.Wait()
	return nil
}

func (s *RelayServiceTCP) handleRelayP2PDataMessage(cmd byte, clientId shared.ClientID, flRun *bridge.FlRunMeta, fromConn *shared.TCPIO) error {
	header, err := shared.ReadDataHeader(fromConn, cmd)
	if err != nil {
		return fmt.Errorf("failed to read p2p data header: %w", err)
	}

	if header.FromClientID != clientId {
		logger.Warn(GLOBALTCP, "", "Header fromClientID %s does not match connected client %s", header.FromClientID.ToString(), clientId.ToString())
	}

	if header.DestinationClientID == nil || *header.DestinationClientID == shared.ZERO_CLIENT_ID {
		if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
			return fmt.Errorf("invalid p2p destination and failed to discard payload: %w", drainErr)
		}
		return fmt.Errorf("invalid p2p destination for client %s", clientId.ToString())
	}

	destinationID := *header.DestinationClientID
	destConn, err := s.waitForConnection(flRun, destinationID)
	if err != nil {
		if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
			return fmt.Errorf("destination unavailable and failed to discard payload: %w", drainErr)
		}
		return err
	}
	return s.streamDataMessage(header, fromConn, destConn)
}

func (s *RelayServiceTCP) handleRelayToCoordinatorMessage(cmd byte, clientId shared.ClientID, flRun *bridge.FlRunMeta, fromConn *shared.TCPIO) error {
	header, err := shared.ReadDataHeader(fromConn, cmd)
	if err != nil {
		return fmt.Errorf("failed to read coordinator-bound header: %w", err)
	}

	if header.FromClientID != clientId {
		logger.Warn(GLOBALTCP, "", "Header fromClientID %s does not match connected client %s", header.FromClientID.ToString(), clientId.ToString())
	}

	if header.DestinationClientID != nil && *header.DestinationClientID != shared.ZERO_CLIENT_ID && *header.DestinationClientID != flRun.CoordinatorID {
		logger.Warn(GLOBALTCP, "", "Discarding coordinator-bound packet from %s with invalid destination %s", clientId.ToString(), header.DestinationClientID.ToString())
		return s.discardPayload(fromConn, header.ContentLength)
	}

	coordinatorConn, err := s.waitForConnection(flRun, flRun.CoordinatorID)
	if err != nil {
		if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
			return fmt.Errorf("coordinator unavailable and failed to discard payload: %w", drainErr)
		}
		return err
	}
	return s.streamDataMessage(header, fromConn, coordinatorConn)
}

// streamDataMessage streams a data message from fromConn to destConn without buffering
// the full payload in memory. Caller must have just finished reading the header from fromConn.
func (s *RelayServiceTCP) streamDataMessage(header *shared.DataPacketHeader, fromConn *shared.TCPIO, destConn *shared.TCPIO) error {
	return destConn.WithHeaderWriteLock(func(io shared.TCPIOInterface) error {
		if err := shared.WriteHeader(io, header); err != nil {
			if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
				return fmt.Errorf("failed writing destination header (%v) and discarding payload failed (%v)", err, drainErr)
			}
			return err
		}

		if err := shared.WriteContentLength(io, header.ContentLength); err != nil {
			if drainErr := s.discardPayload(fromConn, header.ContentLength); drainErr != nil {
				return fmt.Errorf("failed writing destination content length (%v) and discarding payload failed (%v)", err, drainErr)
			}
			return err
		}

		chunk := make([]byte, 32*1024)
		remaining := header.ContentLength
		for remaining > 0 {
			toRead := len(chunk)
			if uint64(toRead) > remaining {
				toRead = int(remaining)
			}
			if err := fromConn.ReadBytes(chunk[:toRead]); err != nil {
				return err
			}
			if err := io.WriteBytes(chunk[:toRead]); err != nil {
				remaining -= uint64(toRead)
				if drainErr := s.discardPayload(fromConn, remaining); drainErr != nil {
					return fmt.Errorf("failed writing payload (%v) and failed discarding remaining payload (%v)", err, drainErr)
				}
				return err
			}
			remaining -= uint64(toRead)
		}
		return nil
	})
}

// broadcastPublicKeyUpdate sends the latest public-key bundle to all connected participants.
func (s *RelayServiceTCP) broadcastPublicKeyUpdate(flRun *bridge.FlRunMeta) error {
	entries, err := flRun.GetPublicKeysSet()
	if err != nil {
		return fmt.Errorf("failed to get public keys set: %w", err)
	}
	headerEntries := make([]shared.PublicKeyBundleEntry, 0, len(entries))
	for clientID, pubKey := range entries {
		headerEntries = append(headerEntries, shared.PublicKeyBundleEntry{
			ClientID:  clientID,
			PublicKey: pubKey,
		})
	}

	bundle := &shared.PublicKeyBundleHeader{
		Cmd:        shared.CMD_SEND_PUBLIC_KEYS,
		Channel:    flRun.Channel,
		NumEntries: uint32(len(headerEntries)),
		Entries:    headerEntries,
	}

	for _, conn := range flRun.GetConnections() {
		err := conn.WithHeaderWriteLock(func(io shared.TCPIOInterface) error {
			return shared.WritePublicKeyBundleHeader(io, bundle)
		})
		if err != nil {
			logConnErr("failed to broadcast public key update", err)
		}
	}
	return nil
}

func (s *RelayServiceTCP) waitForConnection(flRun *bridge.FlRunMeta, clientID shared.ClientID) (*shared.TCPIO, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if conn, ok := flRun.GetConnection(clientID); ok {
			return conn, nil
		}
		// Check if the run is still active
		if !flRun.IsActive() {
			return nil, shared.ErrStopped
		}
		select {
		case <-ticker.C:
		}
	}
}

func (s *RelayServiceTCP) discardPayload(fromConn *shared.TCPIO, contentLength uint64) error {
	if contentLength == 0 {
		return nil
	}
	remaining := contentLength
	chunk := make([]byte, 32*1024)
	for remaining > 0 {
		toRead := len(chunk)
		if uint64(toRead) > remaining {
			toRead = int(remaining)
		}
		if err := fromConn.ReadBytes(chunk[:toRead]); err != nil {
			return err
		}
		remaining -= uint64(toRead)
	}
	return nil
}

func asciiOverview(t *x509.Certificate) string {
	lines := []string{
		"+---------------- Certificate Template ----------------+",
		fmt.Sprintf("| CommonName: %-37s |", t.Subject.CommonName),
	}
	if len(t.DNSNames) > 0 {
		lines = append(lines, fmt.Sprintf("| DNSNames: %-38s |", strings.Join(t.DNSNames, ", ")))
	} else {
		lines = append(lines, "| DNSNames: <none>                                  |")
	}
	lines = append(lines,
		fmt.Sprintf("| NotBefore: %s |", t.NotBefore.Format("2006-01-02")),
		fmt.Sprintf("| NotAfter : %s |", t.NotAfter.Format("2006-01-02")),
		"+-----------------------------------------------------+",
	)
	return strings.Join(lines, "\n")
}
