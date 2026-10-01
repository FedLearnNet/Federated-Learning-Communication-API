// Package bridge provides the shared FL run state used by both the TCP and HTTP
// sub-servers of the relay server. It owns the FL run store and the lifecycle
// signalling that lets relay goroutines exit cleanly when a run is stopped.
package bridge

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	enums "fc_controller/pkg/shared/enums"
	flrun "fc_controller/pkg/shared/flrun"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
)

// FlRunMeta is the relay-server state for one FL run.
// It embeds the shared FlRun base and adds auth keys, TCP connections,
// and lifecycle state so relay goroutines can exit cleanly when a run is stopped.
type FlRunMeta struct {
	*flrun.FlRunBase

	// Auth keys generated at creation time; never changed after init.
	CoordinatorKey     util.APIKey
	ClientId2ClientKey map[shared.ClientID]util.APIKey

	// CA signs the client certificates the controllers of this run present on the
	// TCP connection (mTLS). Generated at creation time; never changed after init.
	CA *util.RunCA
	// signedCertKeys binds each client to the public key (hash) its certificate was signed
	// for, so only one key pair per client is ever accepted for this run.
	signedCertKeys      map[shared.ClientID][sha256.Size]byte
	mutexSignedCertKeys sync.Mutex

	// state tracks the run's lifecycle (Init, Running, Error, Finished).
	// Shared with TCP relay goroutines for automatic graceful exit.
	State atomic.Uint32

	Connections      map[shared.ClientID]*shared.TCPIO
	MutexConnections sync.RWMutex

	// once guards the Stop() call to ensure Shutdown() is called exactly once.
	once sync.Once
}

// newFlRun is the internal factory. Use FlRunStore.Create for external callers.
func newFlRun(maxNumClients int, appVersion enums.AppVersionEnum) (*FlRunMeta, error) {
	channel := shared.NewChannel()
	clientIDs := make([]shared.ClientID, 0, maxNumClients)
	clientId2ClientKey := make(map[shared.ClientID]util.APIKey, maxNumClients)

	for i := 0; i < maxNumClients; i++ {
		clientID, err := shared.NewClientID()
		if err != nil {
			return nil, fmt.Errorf("failed to create client ID: %w", err)
		}
		clientKey, err := util.GenerateAPIKey()
		if err != nil {
			return nil, fmt.Errorf("failed to create client key: %w", err)
		}
		clientIDs = append(clientIDs, clientID)
		clientId2ClientKey[clientID] = clientKey
	}

	var coordinatorID shared.ClientID
	var coordinatorKey util.APIKey
	var err error

	switch appVersion {
	case enums.AppVersionV1:
		// In v1 the coordinator is also a client; promote the last entry.
		coordinatorID = clientIDs[len(clientIDs)-1]
		coordinatorKey = clientId2ClientKey[coordinatorID]
	case enums.AppVersionV2:
		coordinatorID, err = shared.NewClientID()
		if err != nil {
			return nil, fmt.Errorf("failed to create coordinator ID: %w", err)
		}
		coordinatorKey, err = util.GenerateAPIKey()
		if err != nil {
			return nil, fmt.Errorf("failed to create coordinator key: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported app version: %s", appVersion)
	}

	relayKey, err := util.GenerateAPIKey()
	if err != nil {
		return nil, fmt.Errorf("failed to create relay key: %w", err)
	}

	ca, err := util.GenerateRunCA()
	if err != nil {
		return nil, fmt.Errorf("failed to create run CA: %w", err)
	}

	fr := flrun.NewFlRun(channel, coordinatorID, clientIDs, maxNumClients, relayKey, appVersion)

	return &FlRunMeta{
		FlRunBase:          fr,
		CoordinatorKey:     coordinatorKey,
		ClientId2ClientKey: clientId2ClientKey,
		CA:                 ca,
		signedCertKeys:     make(map[shared.ClientID][sha256.Size]byte, maxNumClients+1),
		Connections:        make(map[shared.ClientID]*shared.TCPIO, maxNumClients),
		// State defaults to StateInit (0) via zero initialization
	}, nil
}

// Stop marks the run as finished and signals all relay goroutines to exit cleanly.
// It is idempotent — safe to call from multiple goroutines or multiple times.
//
// The signal is State transition to StateFinished. TCP relay goroutines check
// IsActive() and exit when it becomes false. Shutdown() is also called for
// immediate cleanup of connections.
//
// Why sync.Once: Stop may be called from the HTTP stop endpoint, a server-level
// Shutdown, or future auto-stop paths. We want to ensure Shutdown() runs only once.
func (fr *FlRunMeta) Stop() {
	fr.once.Do(func() {
		// Transition state to finished so relay goroutines see IsActive() = false
		enums.TryMarkFinished(&fr.State)
		fr.Shutdown() // immediate cleanup; TCPIO goroutines will exit when they see !IsActive()
	})
}

// IsActive returns true if the run is still active and can process messages.
// Returns false if the run has been stopped or encountered an error.
func (fr *FlRunMeta) IsActive() bool {
	return enums.IsActive(fr.State.Load())
}

// GetState returns the current lifecycle state of the run.
func (fr *FlRunMeta) GetState() uint32 {
	return fr.State.Load()
}

// IsCoordinator reports whether the given client ID is the coordinator.
func (fr *FlRunMeta) IsCoordinator(clientID shared.ClientID) bool {
	return fr.IsCoordinatorID(clientID)
}

func (fr *FlRunMeta) Authenticate(clientID shared.ClientID, clientKey util.APIKey) bool {
	var expectedKey util.APIKey
	if fr.IsCoordinator(clientID) {
		expectedKey = fr.CoordinatorKey
	} else {
		var exists bool
		expectedKey, exists = fr.ClientId2ClientKey[clientID]
		if !exists {
			// in theory allows timing attacks to get the clientID, but the clientID
			// doesnt really help an attacker without the key, which is protected
			return false
		}
	}
	return clientKey.CheckApiKey(expectedKey)
}

// ErrUnknownClient is returned when a client ID does not belong to the FL run.
var ErrUnknownClient = errors.New("client does not belong to this FL run")

// ErrKeyAlreadyBound is returned when a certificate was already signed for another
// key pair of the same client.
var ErrKeyAlreadyBound = errors.New("a certificate for another key was already signed for this client")

// SignClientCert signs the PEM encoded CSR of a participant of this run and returns the
// PEM encoded client certificate. The CSR's common name must be the client ID.
// Signing the same key again is allowed (e.g. a retry after a lost response), but a
// client can never get certificates for two different keys.
func (fr *FlRunMeta) SignClientCert(clientID shared.ClientID, csrPEM []byte) ([]byte, error) {
	if !fr.IsActive() {
		return nil, shared.ErrStopped
	}
	if _, exists := fr.ClientId2ClientKey[clientID]; !exists && !fr.IsCoordinator(clientID) {
		return nil, ErrUnknownClient
	}

	csr, keyHash, err := util.ParseClientCSR(csrPEM, clientID.ToString())
	if err != nil {
		return nil, err
	}

	fr.mutexSignedCertKeys.Lock()
	defer fr.mutexSignedCertKeys.Unlock()
	if boundKeyHash, exists := fr.signedCertKeys[clientID]; exists && boundKeyHash != keyHash {
		return nil, ErrKeyAlreadyBound
	}
	certPEM, err := fr.CA.SignClientCSR(csr)
	if err != nil {
		return nil, err
	}
	fr.signedCertKeys[clientID] = keyHash
	return certPEM, nil
}

func (fr *FlRunMeta) GetConnections() []*shared.TCPIO {
	fr.MutexConnections.RLock()
	defer fr.MutexConnections.RUnlock()

	connections := make([]*shared.TCPIO, 0, len(fr.Connections))
	for _, conn := range fr.Connections {
		connections = append(connections, conn)
	}
	return connections
}

func (fr *FlRunMeta) GetConnection(clientID shared.ClientID) (*shared.TCPIO, bool) {
	fr.MutexConnections.RLock()
	defer fr.MutexConnections.RUnlock()
	conn, exists := fr.Connections[clientID]
	return conn, exists
}

func (fr *FlRunMeta) AddConnection(clientID shared.ClientID, conn *shared.TCPIO) error {
	fr.MutexConnections.Lock()
	defer fr.MutexConnections.Unlock()
	if _, exists := fr.Connections[clientID]; exists {
		return fmt.Errorf("client ID %s already has a connection", clientID.ToString())
	}
	// Register the run's stop channel so the connection closes and returns
	// ErrStopped on reads/writes when this run is stopped.

	fr.Connections[clientID] = conn
	return nil
}

// Shutdown closes all TCP connections. Called internally by Stop().
func (fr *FlRunMeta) Shutdown() {
	fr.MutexConnections.Lock()
	defer fr.MutexConnections.Unlock()

	for id, conn := range fr.Connections {
		if conn != nil {
			_ = conn.Close()
		}
		delete(fr.Connections, id)
	}
}

// ---------------------------------------------------------------------------
// FlRunStore — thread-safe store of active FL runs, shared by TCP and HTTP servers
// ---------------------------------------------------------------------------

// FlRunStore holds all active FL runs and is the single source of truth shared
// between the TCP relay server and the HTTP API server.
type FlRunStore struct {
	flRuns map[shared.RelayChannel]*FlRunMeta
	lock   sync.RWMutex
}

func NewFlRunStore() *FlRunStore {
	return &FlRunStore{
		flRuns: make(map[shared.RelayChannel]*FlRunMeta),
	}
}

// Create allocates a new FL run, handles channel collision, and adds it to the store.
// Returns the new FlRunMeta so the caller can read CoordinatorID, ClientIDOrder, etc.
func (s *FlRunStore) Create(maxNumClients int, appVersion enums.AppVersionEnum) (*FlRunMeta, error) {
	flRun, err := newFlRun(maxNumClients, appVersion)
	if err != nil {
		return nil, err
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Handle channel collision (astronomically unlikely but handle it gracefully).
	if _, exists := s.flRuns[flRun.Channel]; exists {
		flRun.Channel = shared.NewChannel()
		if _, exists := s.flRuns[flRun.Channel]; exists {
			return nil, fmt.Errorf("channel collision on creation, please retry")
		}
	}

	s.flRuns[flRun.Channel] = flRun
	return flRun, nil
}

// Get looks up a run by channel. Returns nil, false if not found.
func (s *FlRunStore) Get(channel shared.RelayChannel) (*FlRunMeta, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	flRun, exists := s.flRuns[channel]
	return flRun, exists
}

// Stop removes the run from the store and signals all its relay goroutines to exit.
// Connections are closed by FlRunMeta.Stop(), which unblocks any blocked reads.
func (s *FlRunStore) Stop(channel shared.RelayChannel) {
	s.lock.Lock()
	flRun, exists := s.flRuns[channel]
	if exists {
		delete(s.flRuns, channel)
	}
	s.lock.Unlock()

	if exists {
		flRun.Stop()
	}
}

// Shutdown stops every run in the store.
func (s *FlRunStore) Shutdown() {
	s.lock.Lock()
	runs := make([]*FlRunMeta, 0, len(s.flRuns))
	for _, run := range s.flRuns {
		runs = append(runs, run)
	}
	s.flRuns = make(map[shared.RelayChannel]*FlRunMeta)
	s.lock.Unlock()

	for _, run := range runs {
		run.Stop()
	}
}
