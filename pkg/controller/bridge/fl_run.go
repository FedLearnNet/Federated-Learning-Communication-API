package bridge

import (
	"fmt"

	shared_enums "fc_controller/pkg/shared/enums"
	flrun "fc_controller/pkg/shared/flrun"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
)

// FlRunMeta stores the metadata for one federated learning run on one controller.
// It embeds the shared FlRun base and adds controller-specific identity, keys, and
// a thread-safe state machine.
type FlRunMeta struct {
	*flrun.FlRunBase

	OwnClientId   shared.ClientID
	OwnClientKey  util.APIKey
	OwnPrivateKey util.PrivateKey
	IsCoordinator bool

	Version uint32
}

// NewFlRunMeta creates and validates a new FlRunMeta.
func NewFlRunMeta(
	channel shared.RelayChannel,
	clientKey util.APIKey,
	relayKey util.APIKey,
	coordinatorId shared.ClientID,
	ownId shared.ClientID,
	isCoordinator bool,
	maxNumberOfClients int,
	clientIDOrder []shared.ClientID,
	clientPubKey util.PublicKey,
	clientPrivKey util.PrivateKey,
	appVersion shared_enums.AppVersionEnum,
) (*FlRunMeta, error) {
	fr := flrun.NewFlRun(channel, coordinatorId, clientIDOrder, maxNumberOfClients, relayKey, appVersion)
	if err := fr.SetPublicKey(ownId, clientPubKey); err != nil {
		return nil, fmt.Errorf("failed to set own public key: %w", err)
	}
	meta := &FlRunMeta{
		FlRunBase:     fr,
		OwnClientId:   ownId,
		OwnClientKey:  clientKey,
		OwnPrivateKey: clientPrivKey,
		IsCoordinator: isCoordinator,
		Version:       1,
	}
	if err := meta.Validate(); err != nil {
		return nil, err
	}
	return meta, nil
}

// Validate checks the shared FlRun base fields plus controller-specific fields.
func (m *FlRunMeta) Validate() error {
	if m == nil {
		return fmt.Errorf("FlRunMeta is nil")
	}
	if err := m.FlRunBase.Validate(); err != nil {
		return err
	}
	if m.OwnClientId == (shared.ClientID{}) {
		return fmt.Errorf("ownClientId must not be empty")
	}
	if m.OwnPrivateKey == (util.PrivateKey{}) {
		return fmt.Errorf("ownPrivateKey must not be empty")
	}
	if m.OwnClientKey == "" {
		return fmt.Errorf("ownClientKey must not be empty")
	}
	return nil
}

// IsSetupComplete shadows the embedded method to add a nil guard on FlRunMeta itself.
func (m *FlRunMeta) IsSetupComplete() bool {
	if m == nil {
		return false
	}
	return m.FlRunBase.IsSetupComplete()
}

// CountKnownClients shadows the embedded method to add a nil guard on FlRunMeta itself.
func (m *FlRunMeta) CountKnownClients() int {
	if m == nil {
		return 0
	}
	return m.FlRunBase.CountKnownClients()
}
