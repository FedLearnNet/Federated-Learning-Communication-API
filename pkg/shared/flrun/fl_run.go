// Package flrun provides the shared federated learning run state used by both
// the relay server and the controller bridge.
package flrun

import (
	"fmt"
	"sync"

	"fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	"fc_controller/pkg/shared/util"
)

const FLRUNHANDLE = "FL_RUN_HANDLE"

// FlRunBase stores the shared state for one federated learning run.
// Both the relay server (pkg/relayserver/bridge) and the controller bridge (pkg/controller/bridge)
// embed *FlRunBase and add their own role-specific fields on top.
type FlRunBase struct {
	Channel            shared.RelayChannel
	MaxNumberOfClients int

	CoordinatorID shared.ClientID
	ClientIDOrder []shared.ClientID // fixed predetermined participant order

	// RelayKey is the relay server's own API key for this run. The relay server
	// generates it on run creation and sends it back to every connecting client on
	// the TCP handshake.
	// As clients receive this via the trusted orchestration services,
	// this can be used to verify the relay servers identity and prevent MITM attacks.
	// Clients store it here and verify the relay server's
	// identity on connect (mutual auth).
	RelayKey util.APIKey

	ClientId2PublicKey   map[shared.ClientID]util.PublicKey
	CoordinatorPublicKey *util.PublicKey
	MutexPubKey          sync.RWMutex
	AppVersion           enums.AppVersionEnum
}

// NewFlRun creates a FlRun with initialized maps.
func NewFlRun(
	channel shared.RelayChannel,
	coordinatorID shared.ClientID,
	clientIDOrder []shared.ClientID,
	maxNumberOfClients int,
	relayKey util.APIKey,
	appVersion enums.AppVersionEnum,
) *FlRunBase {
	return &FlRunBase{
		Channel:            channel,
		CoordinatorID:      coordinatorID,
		ClientIDOrder:      clientIDOrder,
		MaxNumberOfClients: maxNumberOfClients,
		RelayKey:           relayKey,
		ClientId2PublicKey: make(map[shared.ClientID]util.PublicKey, maxNumberOfClients),
		AppVersion:         appVersion,
	}
}

// Validate checks that all shared required fields are set and internally consistent.
// Callers that embed *FlRun should call this from their own Validate method.
func (fr *FlRunBase) Validate() error {
	if fr == nil {
		return fmt.Errorf("FlRun is nil")
	}
	if fr.Channel == (shared.RelayChannel{}) {
		return fmt.Errorf("channel must not be empty")
	}
	if fr.CoordinatorID == (shared.ClientID{}) {
		return fmt.Errorf("coordinatorID must not be empty")
	}
	if fr.MaxNumberOfClients <= 0 {
		return fmt.Errorf("maxNumberOfClients must be > 0")
	}
	if len(fr.ClientIDOrder) != fr.MaxNumberOfClients {
		return fmt.Errorf("clientIDOrder must have exactly maxNumberOfClients entries")
	}
	if len(fr.ClientId2PublicKey) > fr.MaxNumberOfClients {
		return fmt.Errorf("known clients exceed maxNumberOfClients")
	}
	if err := validateClientOrderAgainstMap(fr.ClientIDOrder, fr.ClientId2PublicKey); err != nil {
		return err
	}
	return nil
}

// GetPublicKey returns the public key for a given client or coordinator ID.
func (fr *FlRunBase) GetPublicKey(clientID shared.ClientID) (util.PublicKey, bool) {
	fr.MutexPubKey.RLock()
	defer fr.MutexPubKey.RUnlock()

	if pk, exists := fr.ClientId2PublicKey[clientID]; exists {
		return pk, true
	}
	if fr.CoordinatorID == clientID && fr.CoordinatorPublicKey != nil {
		return *fr.CoordinatorPublicKey, true
	}
	return util.PublicKey{}, false
}

// GetPublicKeysSet returns a combined map of all known public keys (clients and coordinator).
func (fr *FlRunBase) GetPublicKeysSet() (map[shared.ClientID]util.PublicKey, error) {
	fr.MutexPubKey.RLock()
	defer fr.MutexPubKey.RUnlock()

	keys := make(map[shared.ClientID]util.PublicKey, len(fr.ClientId2PublicKey)+1)
	for clientID, pk := range fr.ClientId2PublicKey {
		keys[clientID] = pk
	}
	if fr.CoordinatorPublicKey != nil {
		if existing, exists := keys[fr.CoordinatorID]; exists && existing != *fr.CoordinatorPublicKey {
			return nil, fmt.Errorf("coordinator public key mismatch")
		}
		keys[fr.CoordinatorID] = *fr.CoordinatorPublicKey
	}
	return keys, nil
}

// SetPublicKeys merges a relay-server broadcast bundle into the known key set.
// It is idempotent for keys that are already stored:
//   - app v1: len(entries) > MaxNumberOfClients → error
//   - app v2: len(entries) > MaxNumberOfClients+1 → error (coordinator separate from clients)
//   - entry already known with the same key → skip
//   - entry already known with a different key → error (possible MITM)
//   - entry not yet known → store it
func (fr *FlRunBase) SetPublicKeys(entries []shared.PublicKeyBundleEntry) error {
	maxLen := fr.MaxNumberOfClients
	if !fr.coordinatorIsParticipant() {
		maxLen++ // allow one extra for the coordinator key if it's separate from clients (app v2)
	}
	if len(entries) > maxLen { // +1 for coordinator key
		return fmt.Errorf("received %d public key entries but can at most have %d", len(entries), maxLen)
	}

	fr.MutexPubKey.Lock()
	defer fr.MutexPubKey.Unlock()

	for _, entry := range entries {
		if fr.IsCoordinatorID(entry.ClientID) {
			if fr.CoordinatorPublicKey != nil {
				if *fr.CoordinatorPublicKey != entry.PublicKey {
					return fmt.Errorf("coordinator public key mismatch for %s", entry.ClientID.ToString())
				}
				continue // same key, already known
			}
			fr.CoordinatorPublicKey = &entry.PublicKey
			if fr.coordinatorIsParticipant() {
				fr.ClientId2PublicKey[entry.ClientID] = entry.PublicKey
			}
		} else {
			if existing, exists := fr.ClientId2PublicKey[entry.ClientID]; exists {
				if existing != entry.PublicKey {
					return fmt.Errorf("public key mismatch for client %s", entry.ClientID.ToString())
				}
				continue // same key, already known
			}
			fr.ClientId2PublicKey[entry.ClientID] = entry.PublicKey
		}
	}
	return nil
}

// SetPublicKey stores the public key for a client or coordinator ID.
func (fr *FlRunBase) SetPublicKey(clientID shared.ClientID, publicKey util.PublicKey) error {
	if fr.IsCoordinatorID(clientID) {
		return fr.setCoordinatorPublicKey(clientID, publicKey)
	}
	return fr.setClientPublicKey(clientID, publicKey)
}

func (fr *FlRunBase) setCoordinatorPublicKey(coordinatorID shared.ClientID, publicKey util.PublicKey) error {
	fr.MutexPubKey.Lock()
	defer fr.MutexPubKey.Unlock()
	if fr.CoordinatorID != coordinatorID {
		return fmt.Errorf("coordinator ID does not match")
	}
	if fr.CoordinatorPublicKey != nil {
		return fmt.Errorf("coordinator public key already set")
	}
	fr.CoordinatorPublicKey = &publicKey
	// App v1: coordinator is also a client participant (present in ClientIDOrder).
	// Store the key in ClientId2PublicKey as well so participant counting works uniformly.
	if fr.coordinatorIsParticipant() {
		fr.ClientId2PublicKey[coordinatorID] = publicKey
	}
	return nil
}

func (fr *FlRunBase) setClientPublicKey(clientID shared.ClientID, publicKey util.PublicKey) error {
	fr.MutexPubKey.Lock()
	defer fr.MutexPubKey.Unlock()
	if _, exists := fr.ClientId2PublicKey[clientID]; exists {
		return fmt.Errorf("client %s already has a public key set", clientID.ToString())
	}
	fr.ClientId2PublicKey[clientID] = publicKey
	return nil
}

// IsCoordinatorID reports whether the given client ID is the coordinator of this run.
func (fr *FlRunBase) IsCoordinatorID(clientID shared.ClientID) bool {
	return fr.CoordinatorID == clientID
}

// coordinatorIsParticipant reports whether the coordinator is also a client participant
// (i.e. included in ClientIDOrder). True for app v1, false for app v2.
func (fr *FlRunBase) coordinatorIsParticipant() bool {
	for _, id := range fr.ClientIDOrder {
		if id == fr.CoordinatorID {
			if fr.AppVersion != enums.AppVersionV1 {
				logger.Error(FLRUNHANDLE, "", "Coordinator ID is in ClientIDOrder but AppVersion is not v1")
			}
			return true
		}
	}
	if fr.AppVersion == enums.AppVersionV1 {
		logger.Error(FLRUNHANDLE, "", "AppVersion is v1 but Coordinator ID is not in ClientIDOrder")
	}
	return false
}

// IsSetupComplete reports whether all expected keys are known:
// the coordinator/aggregator key and all participant keys.
// In app v1 the coordinator is also a participant so its key appears in both maps;
// in app v2 it is aggregator-only and only in CoordinatorPublicKey.
func (fr *FlRunBase) IsSetupComplete() bool {
	if fr == nil {
		return false
	}
	return fr.CoordinatorPublicKey != nil && len(fr.ClientId2PublicKey) == fr.MaxNumberOfClients
}

// CountKnownClients returns the number of participants for which a public key is known.
func (fr *FlRunBase) CountKnownClients() int {
	if fr == nil {
		return 0
	}
	return len(fr.ClientId2PublicKey)
}

// validateClientOrderAgainstMap verifies the order slice has no duplicates and
// every client in the map is present in the order.
func validateClientOrderAgainstMap(order []shared.ClientID, clientId2PublicKey map[shared.ClientID]util.PublicKey) error {
	seen := make(map[shared.ClientID]struct{}, len(order))
	for _, clientID := range order {
		if _, exists := seen[clientID]; exists {
			return fmt.Errorf("client order contains duplicate client IDs")
		}
		seen[clientID] = struct{}{}
	}
	for clientID := range clientId2PublicKey {
		if _, exists := seen[clientID]; !exists {
			return fmt.Errorf("client map contains client ID not in client order")
		}
	}
	return nil
}

// validateClientMapAgainstOrder verifies every client in the map is present in the fixed order.
func validateClientMapAgainstOrder(clientId2PublicKey map[shared.ClientID]util.PublicKey, order []shared.ClientID) error {
	seen := make(map[shared.ClientID]struct{}, len(order))
	for _, clientID := range order {
		seen[clientID] = struct{}{}
	}
	for clientID := range clientId2PublicKey {
		if _, exists := seen[clientID]; !exists {
			return fmt.Errorf("client map contains client ID not in client order")
		}
	}
	return nil
}
