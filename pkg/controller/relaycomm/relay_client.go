package relaycomm

import (
	"crypto/tls"
	bridge "fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/learningcomm"
	models "fc_controller/pkg/controller/models"
	shared_enums "fc_controller/pkg/shared/enums"
	shared_link "fc_controller/pkg/shared/link"
	logger "fc_controller/pkg/shared/logger"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

const RELAYCLIENT string = "RELAY_CLIENT"

// RelayClient represents a locally running relay server client which:
// LOCAL -> GLOBAL: Relay messages to other clients/the coordinator
//
//	If using app version 1 queries another local server (An app) for messages to relay
//	If using app version 2 exposes API that a local server (app) uses to post messages
//
// GLOBAL -> LOCAL: Receive messages from the global relay server which is relaying for
// other clients/the coordinator.
//
//	if using app version 1 posts them to the local server (app)
//	if using app version 2 stores them in a message store
//		The local server (app) can then retrieve them via an API endpoint which queries
//		that message store
type RelayClient struct {
	/*
		Global relay information
	*/

	// Address of the global relay server
	globalAddress string

	/*
		Local App related information
	*/
	// Address of the local server
	// Only used in appVersion v1, empty string in v2
	appUrl string

	// Interval in which the local server is being queried
	queryInterval time.Duration

	/*
		SMPC-related parameters
	*/

	// Stores received shards from clients
	// first key is the memo, the second key is the clientID
	// Handles SMPC_SEND_SMPC_DATA commands
	// Therefore handles storing data for the FIRST aggregation per client of other clients shards
	q smpcShardStore

	// Stores received AGGREGATED shards from clients
	// first key is the memo, the second key is the clientID
	// Handles SMPC_SEND_SMPC_AGG commands
	// Therefore handles storing data for the SECOND aggregation per
	// aggregated data each client produces and sends in the first aggregation step
	// Therefore only used by the coordinator
	qAggregator smpcShardStore

	// Protects q and qAggregator from concurrent goroutine access
	// (each incoming message is handled in its own goroutine via go s.handleMessage(...))
	smpcMu sync.Mutex

	/*
		Internal variables
	*/

	// Callback function to update information whenever the app progress, app status or app message changes
	updateCB learningcomm.UpdateCB

	// Contains config for encrypted TLS connection (nil is unencrypted)
	tlsConfig *tls.Config

	// Socket connection to the global relay server
	globalConnection *shared_link.TCPIO

	/*
		Variables shared with code related to the app communication
	*/
	// Handle to the FL run, used for storing and updating shared information about the FL run
	// like client public keys, coordinator ID, incoming and outgoing messages
	runHandle *bridge.FLRunHandle
}

// smpcShardStore is used for both the first aggregation round by each client and the
// second aggregation round by the coordinator
type smpcShardStore map[string]map[shared_link.ClientID][]models.SMPCMessageWrapper

const LOCAL = "LOCAL"

func NewClient(interval time.Duration, runHandle *bridge.FLRunHandle, appUrl string,
	channel shared_link.RelayChannel, maxNumClients int,
	globalAddress string, mode string, tlsMode string) (*RelayClient, error) {
	// Do not initialize pointer fields here; keep them nil so they can be
	// configured/generated later (e.g. TLS config, keypair generation).

	s := &RelayClient{
		queryInterval: interval,
		appUrl:        appUrl,
		runHandle:     runHandle,
		globalAddress: strings.Trim(strings.TrimRight(globalAddress, "/"), "\""),
		q:             make(smpcShardStore),
		qAggregator:   make(smpcShardStore),
	}

	if err := s.configureTLS(mode, tlsMode); err != nil {
		return nil, err
	}

	// Ensure we got a valid FlRunMeta or everything breaks
	if runHandle == nil {
		// Fail
		return nil, fmt.Errorf("runHandle is required")
	} else {
		// Validate
		if err := runHandle.Meta.FlRunBase.Validate(); err != nil {
			return nil, fmt.Errorf("invalid FLRunMeta: %s", err.Error())
		}
	}

	return s, nil
}

func (c *RelayClient) configureTLS(mode string, tlsMode string) error {
	if mode == "" {
		mode = "prod"
	}
	if mode != "prod" && mode != "dev" {
		return fmt.Errorf("unsupported relay mode %q", mode)
	}

	if tlsMode == "" {
		if mode == "prod" {
			tlsMode = "on"
		} else {
			tlsMode = "self-signed"
		}
	}

	// Hard security boundary
	if mode == "prod" && tlsMode != "on" {
		return fmt.Errorf("invalid TLS mode %q for production mode", tlsMode)
	}

	switch tlsMode {
	case "off":
		c.tlsConfig = nil
	case "self-signed":
		logger.Warn(RELAYCLIENT, "", "Using self-signed TLS. Connection is vulnerable to MITM attacks!")
		c.tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS13,
		}
	case "on":
		c.tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS13,
		}
	default:
		return fmt.Errorf("unsupported TLS mode %q", tlsMode)
	}

	return nil
}

// StartClient connects to the global relay server and register this client,
// also announcing own public key
// Starts loading messages from the global relay server into the message store
// and starts the routine that queries the message queue for
// outgoing messages to send to the global relay server
func (c *RelayClient) StartClient() error {
	if c.runHandle.GetState() != shared_enums.StateInit || c.globalConnection != nil {
		return fmt.Errorf("Client is already connected. Flow error")
	}

	logger.Debug(RELAYCLIENT, "", "Connecting to %s...", c.globalAddress)

	// Fail fast if the server isn't reachable within 10 seconds
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
	}

	var err error
	var conn net.Conn

	if c.tlsConfig != nil {
		// tls.DialWithDialer uses our timeout wrapper
		conn, err = tls.DialWithDialer(dialer, "tcp", c.globalAddress, c.tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", c.globalAddress)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to global relay: %w", err)
	}

	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	logger.Debug(RELAYCLIENT, "", "Successfully connected to global relay, authenticating...")
	// Temporary deadline to avoid hanging on authentication
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	conntcp := shared_link.NewTCPIO(conn, &c.runHandle.State)

	// Send the connection initialization directly while still holding the mutex
	// This MUST be the first message
	// Auth TOWARDS the relay server
	connectionHeader := shared_link.NewConnectionSetupHeader(c.runHandle.Meta.Channel, c.runHandle.Meta.OwnClientId, c.runHandle.Meta.OwnClientKey)
	if err := conntcp.WithHeaderWriteLock(func(io shared_link.TCPIOInterface) error {
		return shared_link.WriteConnectionSetupHeader(io, connectionHeader)
	}); err != nil {
		return err
	}
	logger.Debug(RELAYCLIENT, "", "Sent authentication message to global relay, waiting for response...")
	// Mutual auth: read the relay server's key and verify it matches what the outside service told us.
	receivedRelayKey, err := shared_link.ReadConnectionSetupResponse(conntcp)
	if err != nil {
		return fmt.Errorf("failed to read relay server auth response: %w", err)
	}
	if !receivedRelayKey.CheckApiKey(c.runHandle.Meta.RelayKey) {
		return fmt.Errorf("relay server identity verification failed: relay key mismatch")
	}
	logger.Info(RELAYCLIENT, "", "Successfully authenticated with global relay at %s", c.globalAddress)

	// Connection successful, update state and remove deadline
	_ = conn.SetDeadline(time.Time{}) // Clear the deadline
	c.runHandle.MarkRunning()

	// Announce our public key
	pubKey, exists := c.runHandle.Meta.GetPublicKey(c.runHandle.Meta.OwnClientId)
	if !exists {
		return fmt.Errorf("own public key not found in FLRunMeta; this should never happen since we always initialize the FLRunMeta with the own client ID and public key")
	}
	pubkeyHeader := shared_link.NewPublicKeyAnnouncementHeader(c.runHandle.Meta.Channel, pubKey)
	if err := conntcp.WithHeaderWriteLock(func(io shared_link.TCPIOInterface) error {
		return shared_link.WritePublicKeyAnnouncementHeader(io, pubkeyHeader)
	}); err != nil {
		return err
	}

	c.globalConnection = conntcp
	conn = nil // Ownership transferred to globalConnection.
	// makes sure we don't close the connection in the defer above,
	// since it's now managed by globalConnection
	logger.Info(RELAYCLIENT, "", "Connected to global relay at %s", c.globalAddress)

	// READ IN (LISTEN)
	go c.readIncomming()

	// WRITE OUT
	go c.watchOutgoing()
	return nil
}

// Stop shuts down the relay client by closing the connection itself
func (c *RelayClient) Stop() {
	if c.globalConnection != nil {
		c.globalConnection.Stop()
		c.globalConnection = nil
	}
}

// Helpers
func memoToBinaryMemo(memo *string) (byte, []byte) {
	var memoStr string
	if memo == nil {
		memoStr = ""
	} else if len(*memo) > 255 {
		logger.Warn(LOCAL, "", "Memo length exceeds 255 bytes and will be truncated: %s", *memo)
		memoStr = (*memo)[:255]
	} else {
		memoStr = *memo
	}
	var memosize byte = 0
	var memoBytes []byte
	if memoStr != "" {
		memoBytes = []byte(memoStr)
		memosize = byte(len(memoBytes))
	}
	return memosize, memoBytes
}
