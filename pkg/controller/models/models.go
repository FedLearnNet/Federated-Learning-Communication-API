package models

import (
	"errors"
	"fmt"

	shared_enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	util "fc_controller/pkg/shared/util"
)

// AutoCommIDPrefix is the prefix for communication IDs automatically assigned by the controller
// when an app sends a message without an explicit communicationId.
const AutoCommIDPrefix = "AUTOMATIC_COMM_ID_"

// If an app requests this specific comm id this is interpreted as the app requesting
// messages that were sent with using the automatic comm id system
// using the AutoCommIDPrefix
const RequestAutoCommId = "#AUTOMATIC"

// FLExperiment holds data of a Federated Learning Experiment (which is a single execution of a Federated Learning Project)
type FLExperiment struct {
	// For connecting to the global relay server
	Channel   string          `json:"channel"`
	ClientId  shared.ClientID `json:"clientId"`
	ClientKey util.APIKey     `json:"clientKey"`
	RelayKey  util.APIKey     `json:"relayKey"` // relay server's auth key, verified during TCP handshake

	// Globally shared information about this fl run
	RunId          string            `json:"runId"`
	CoordinatorId  shared.ClientID   `json:"coordinatorId"`
	MaxNumClients  int               `json:"maxNumClients"`
	OrderClientIds []shared.ClientID `json:"orderClientIds"`

	// For connecting to the local app
	AppKey util.APIKey `json:"appKey"`
	// in app v1 we poll the app, in app v2 the app POSTs to us
	AppVersion    shared_enums.AppVersionEnum `json:"appVersion"`
	AppControlURL *string                     `json:"appControlUrl,omitempty"` // app v1 only

	// Related to the app version and how we should relay
	// In v1 we emulate the websocket connection to the
	// local learning api
	LearningApiWsSuffixURL *string `json:"learningApiWsSuffixURL,omitempty"` // app v1 only
	LearningApiWsToken     *string `json:"learningApiWsToken,omitempty"`     // app v1 only
}

type LocalSetupBody struct {
	ID            shared.ClientID   `json:"id"`
	Master        bool              `json:"master"` // Legacy, kept for compatibility reasons
	Coordinator   bool              `json:"coordinator"`
	CoordinatorID shared.ClientID   `json:"coordinatorID"`
	Clients       []shared.ClientID `json:"clients"`
}

type UpdateRunDTO struct {
	RunId    int64    `json:"runId"`
	Status   string   `json:"status"`
	Error    *string  `json:"error,omitempty"`
	Progress *float64 `json:"progress,omitempty"`
}

type RunMessageLogDTO struct {
	RunId      int64   `json:"runId"`
	Severity   string  `json:"severity"`
	Message    string  `json:"message"`
	Caller     *string `json:"caller,omitempty"`
	StackTrace *string `json:"stackTrace,omitempty"`
	Group      *string `json:"group,omitempty"`
}

// NormalizedFLExperiment is the validated, typed representation of a FLExperiment HTTP request body.
// All fields are guaranteed to be valid when this struct is returned.
type NormalizedFLExperiment struct {
	// Relay connection
	Channel   shared.RelayChannel
	ClientId  shared.ClientID
	ClientKey util.APIKey
	RelayKey  util.APIKey

	// Run metadata
	RunId          string
	CoordinatorId  shared.ClientID
	OwnPrivateKey  util.PrivateKey
	OwnPublicKey   util.PublicKey
	MaxNumClients  int
	OrderClientIds []shared.ClientID

	// App connection
	AppKey     util.APIKey
	AppVersion shared_enums.AppVersionEnum

	// App v1 only (nil or empty string when AppVersion != v1)
	AppControlURL          string
	LearningApiWsSuffixURL *string
	LearningApiWsToken     *string
}

// Normalize validates the FLExperiment and returns a NormalizedFLExperiment.
// It returns an error if any required field is missing, malformed, or logically inconsistent.
func (e *FLExperiment) Normalize() (*NormalizedFLExperiment, error) {
	// --- Channel ---
	if e.Channel == "" {
		return nil, errors.New("channel must not be empty")
	}
	channel, err := shared.ChannelFromString(e.Channel)
	if err != nil {
		return nil, fmt.Errorf("invalid channel: %w", err)
	}

	// --- ClientId ---
	if e.ClientId == (shared.ClientID{}) || e.ClientId == shared.ZERO_CLIENT_ID {
		return nil, errors.New("clientId must not be zero")
	}

	// --- ClientKey ---
	if e.ClientKey == "" {
		return nil, errors.New("clientKey must not be empty")
	}

	// --- RelayKey ---
	if e.RelayKey == "" {
		return nil, errors.New("relayKey must not be empty")
	}

	// --- RunId ---
	if e.RunId == "" {
		return nil, errors.New("runId must not be empty")
	}

	// --- CoordinatorId ---
	if e.CoordinatorId == (shared.ClientID{}) || e.CoordinatorId == shared.ZERO_CLIENT_ID {
		return nil, errors.New("coordinatorId must not be zero")
	}

	// --- MaxNumClients ---
	if e.MaxNumClients <= 0 {
		return nil, fmt.Errorf("maxNumClients must be positive, got %d", e.MaxNumClients)
	}

	// --- OrderClientIds ---
	if len(e.OrderClientIds) != e.MaxNumClients {
		return nil, fmt.Errorf("length of orderClientIds must be equal to maxNumClients (%d), got %d", e.MaxNumClients, len(e.OrderClientIds))
	}

	// --- AppKey ---
	if e.AppKey == "" {
		return nil, errors.New("appKey must not be empty")
	}

	// --- AppVersion ---
	if err := e.AppVersion.Validate(); err != nil {
		return nil, fmt.Errorf("appVersion invalid: %w", err)
	}

	// --- App v1 fields: required when AppVersion is v1, forbidden otherwise ---
	var appControlUrlString string // to satisfy the type system, this value is not used when AppVersion != v1
	if e.AppVersion == shared_enums.AppVersionV1 {
		if e.AppControlURL == nil || *e.AppControlURL == "" {
			return nil, errors.New("appControlUrl is required for app v1")
		}
		appControlUrlString = *e.AppControlURL
		if e.LearningApiWsSuffixURL == nil || *e.LearningApiWsSuffixURL == "" {
			return nil, errors.New("learningApiWsSuffixURL is required for app v1")
		}
		if e.LearningApiWsToken == nil || *e.LearningApiWsToken == "" {
			return nil, errors.New("learningApiWsToken is required for app v1")
		}
	} else {
		if e.AppControlURL != nil {
			return nil, errors.New("appControlUrl must not be set for app v2")
		}
		appControlUrlString = "" // to satisfy the type system, this value is not used when AppVersion != v1
		if e.LearningApiWsSuffixURL != nil {
			return nil, errors.New("learningApiWsSuffixURL must not be set for app v2")
		}
		if e.LearningApiWsToken != nil {
			return nil, errors.New("learningApiWsToken must not be set for app v2")
		}
	}

	// --- Generate the key pair for this client ---
	privKey, pubKey, err := util.GenerateKeyPair()
	if err != nil || privKey.PrivateKey == nil || !pubKey.Valid() {
		return nil, errors.New("failed to generate ephemeral key pair for client")
	}

	return &NormalizedFLExperiment{
		Channel:                channel,
		ClientId:               e.ClientId,
		ClientKey:              e.ClientKey,
		RelayKey:               e.RelayKey,
		CoordinatorId:          e.CoordinatorId,
		MaxNumClients:          e.MaxNumClients,
		OrderClientIds:         e.OrderClientIds,
		OwnPrivateKey:          privKey,
		OwnPublicKey:           pubKey,
		AppKey:                 e.AppKey,
		AppVersion:             e.AppVersion,
		AppControlURL:          appControlUrlString,
		RunId:                  e.RunId,
		LearningApiWsSuffixURL: e.LearningApiWsSuffixURL,
		LearningApiWsToken:     e.LearningApiWsToken,
	}, nil
}

// ValidationError indicates invalid input data
type ValidationError struct {
	Message string
}

func (e ValidationError) Error() string {
	return e.Message
}

// ConflictError indicates a resource conflict (e.g., duplicate appKey, channel, or appUrl)
type ConflictError struct {
	Message string
}

func (e ConflictError) Error() string {
	return e.Message
}

// NotFoundError indicates a requested resource was not found
type NotFoundError struct {
	Message string
}

func (e NotFoundError) Error() string {
	return e.Message
}
