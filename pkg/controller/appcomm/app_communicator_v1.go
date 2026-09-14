// app [<--] controller <-- global relay server   (incoming: relay posts to app)
// app [--> controller --> global relay server   (outgoing: app is polled, result relayed)
// Handles all app communication for v1 apps. One instance per FL run.
package appcomm

import (
	"bytes"
	"encoding/json"
	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/codec"
	"fc_controller/pkg/controller/enums"
	"fc_controller/pkg/controller/learningcomm"
	"fc_controller/pkg/controller/models"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// setupPayload is the JSON body sent to the app's POST /setup endpoint.
type setupPayload struct {
	ID            shared.ClientID   `json:"id"`
	Coordinator   bool              `json:"coordinator"`
	CoordinatorID shared.ClientID   `json:"coordinatorID"`
	Clients       []shared.ClientID `json:"clients"`
}

const logV1 = "APP_COMM_V1"

// AppCommunicatorV1 handles communication with a v1 app for one FL run.
//   - Watches the run's incoming MessageStore; POSTs each arriving message to the app's /data endpoint.
//   - Polls the app's /status endpoint; enqueues raw outgoing messages to the run's MessageQueue
//     for the RelayClient to pick up, encrypt, and send.
type AppCommunicatorV1 struct {
	appUrl        string
	queryInterval time.Duration
	updateCB      learningcomm.UpdateCB
	handle        *bridge.FLRunHandle
}

// NewAppCommunicatorV1 creates a new V1 app communicator for a single FL run.
func NewAppCommunicatorV1(appUrl string, queryInterval time.Duration, updateCB learningcomm.UpdateCB, handle *bridge.FLRunHandle) *AppCommunicatorV1 {
	return &AppCommunicatorV1{
		appUrl:        appUrl,
		queryInterval: queryInterval,
		handle:        handle,
		updateCB:      updateCB,
	}
}

// Start POSTs setup to the app, then launches the incoming watcher and outgoing poll loop.
// The poll loop blocks; call Stop to shut both down.
func (a *AppCommunicatorV1) StartClient() error {
	if err := a.sendSetup(); err != nil {
		return fmt.Errorf("failed to send setup to app: %w", err)
	}
	go a.watchIncoming()
	return a.pollLoop()
}

// sendSetup POSTs the run's identity and participant list to the app's /setup endpoint.
// Uses exponential backoff; fails after 5 attempts.
func (a *AppCommunicatorV1) sendSetup() error {
	meta := a.handle.Meta
	payload := setupPayload{
		ID:            meta.OwnClientId,
		Coordinator:   meta.IsCoordinator,
		CoordinatorID: meta.CoordinatorID,
		Clients:       meta.ClientIDOrder,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal setup payload: %w", err)
	}

	maxTries := 5
	for tries := 0; tries < maxTries; tries++ {
		resp, err := http.DefaultClient.Post(a.appUrl+"/setup", "application/json", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				logger.Info(logV1, "", "Setup successfully sent to app")
				return nil
			}
			logger.Warn(logV1, "", "POST /setup returned status %d", resp.StatusCode)
		} else {
			logger.Warn(logV1, "", "POST /setup error: %s", err.Error())
		}
		if tries < maxTries-1 {
			wait := time.Duration(1<<tries) * time.Second
			logger.Warn(logV1, "", "Retrying setup in %v (%d/%d)", wait, tries+1, maxTries)
			time.Sleep(wait)
		}
	}
	return fmt.Errorf("POST /setup failed after %d attempts", maxTries)
}

// watchIncoming waits for the RelayClient to signal IncomingNotify, then pulls all pending
// messages from the MessageStore and POSTs each one to the app's /data endpoint.
func (a *AppCommunicatorV1) watchIncoming() {
	for range a.handle.IncomingNotify {
		// SafePull checks if run is active and blocks cleanup (MarkInactive)
		// from proceeding until this pull completes.
		messages := a.handle.SafePull(nil, nil, nil, nil)
		if messages == nil {
			// Run marked inactive; stop processing
			return
		}
		for _, msg := range messages {
			srcMode, err := enums.NormalizeSerializationModeByte(msg.SrcSerializationModeByte)
			if err != nil {
				logger.Error(logV1, "", "unknown serialization mode in incoming message: %v", err)
				continue
			}
			srcCompression, err := enums.NormalizeCompressionModeByte(msg.SrcCompressionModeByte)
			if err != nil {
				logger.Error(logV1, "", "unknown compression mode in incoming message: %v", err)
				continue
			}
			var dataBytes []byte
			if srcMode == enums.SerializationPickle {
				dataBytes = msg.Data.([]byte)
			} else {
				dataBytes, err = codec.DecodeEncode(msg.Data, nil, srcMode, srcCompression, nil, enums.SerializationJSON, enums.CompressionNone)
				if err != nil {
					logger.Error(logV1, "", "failed to transcode incoming message to JSON: %v", err)
					continue
				}
			}
			a.postToApp(msg.CommunicationId, msg.FromClientId, dataBytes)
		}
	}
}

type lastPollState struct {
	progress *float64
	message  *string
	state    *string
}

// errAppFinished is returned by singlePoll when the app reports it has finished.
// pollLoop treats this as a clean exit, not an error.
var errAppFinished = fmt.Errorf("app finished")

// pollLoop continuously polls the app's /status endpoint for outgoing messages.
func (a *AppCommunicatorV1) pollLoop() error {
	last := &lastPollState{}
	for {
		if !a.handle.IsActive() {
			return nil
		}
		if err := a.singlePoll(last); err != nil {
			if err == errAppFinished {
				return nil
			}
			return err
		}
		time.Sleep(a.queryInterval)
	}
}

// statusResponseV1 is the JSON structure returned by the app's /status endpoint.
type statusResponseV1 struct {
	Size      int64 `json:"size"`
	Available bool  `json:"available"`
	Finished  bool  `json:"finished"`

	Progress *float64 `json:"progress,omitempty"`
	Message  *string  `json:"message,omitempty"`
	State    *string  `json:"state,omitempty"`

	Destination *shared.ClientID `json:"destination,omitempty"`
	Memo        *string          `json:"memo,omitempty"`

	SMPC *struct {
		Operation     string  `json:"operation"`
		Serialization *string `json:"serialization"` // unused; app V1 always uses JSON for SMPC
		Shards        *int    `json:"shards"`
		Exponent      *uint8  `json:"exponent"`
	} `json:"smpc,omitempty"`

	DP *struct {
		SerializationDP *string  `json:"serializationDP"` // unused; app V1 always uses JSON for DP
		Noisetype       *string  `json:"noisetype"`
		Epsilon         *float64 `json:"epsilon"`
		Delta           *float64 `json:"delta"`
		Sensitivity     *float64 `json:"sensitivity"`
		ClippingVal     *float64 `json:"clippingval"`
	} `json:"dp,omitempty"`
}

// singlePoll performs one poll cycle: reads /status, fetches /data if available, enqueues outgoing.
// last is owned by pollLoop and persists across calls.
func (a *AppCommunicatorV1) singlePoll(last *lastPollState) error {
	statusResp, err := http.DefaultClient.Get(a.appUrl + "/status")
	if err != nil {
		return err
	}
	defer statusResp.Body.Close()
	if statusResp.StatusCode != 200 {
		return fmt.Errorf("expected status 200 from /status, got %d", statusResp.StatusCode)
	}

	statusBody, err := io.ReadAll(statusResp.Body)
	if err != nil {
		return err
	}

	statusResponse := &statusResponseV1{}
	if err = json.Unmarshal(statusBody, statusResponse); err != nil {
		return err
	}

	if statusResponse.Available {
		logger.Info(logV1, "", "%s/status: New data available", a.appUrl)
		if err := a.fetchAndEnqueue(statusResponse); err != nil {
			return err
		}
	}

	if statusResponse.Finished {
		logger.Info(logV1, "", "%s/status: Finished", a.appUrl)
		if a.updateCB != nil {
			finished := "finished"
			a.updateCB(statusResponse.Message, nil, &finished, learningcomm.LogMessage)
			a.updateCB(statusResponse.Message, statusResponse.Progress, &finished, learningcomm.UpdateRun)
		}
		return errAppFinished
	}

	if a.updateCB != nil {
		// Log the change
		if shared.PtrStringChanged(last.message, statusResponse.Message) ||
			shared.PtrFloat64Changed(last.progress, statusResponse.Progress) ||
			shared.PtrStringChanged(last.state, statusResponse.State) {
			a.updateCB(statusResponse.Message, nil, statusResponse.State, learningcomm.LogMessage)
			a.updateCB(statusResponse.Message, statusResponse.Progress, statusResponse.State, learningcomm.UpdateRun)
		}
	}

	last.message = statusResponse.Message
	last.progress = statusResponse.Progress
	last.state = statusResponse.State

	return nil
}

// fetchAndEnqueue reads the raw payload from /data, wraps it into an outgoing Message, and enqueues
// it for the RelayClient to pick up. The RelayClient is responsible for E2E encryption and sending.
func (a *AppCommunicatorV1) fetchAndEnqueue(status *statusResponseV1) error {
	timeReceived := time.Now()
	dataResp, err := http.DefaultClient.Get(a.appUrl + "/data")
	if err != nil {
		return err
	}
	defer dataResp.Body.Close()
	if dataResp.StatusCode != 200 {
		return fmt.Errorf("expected status 200 from /data, got %d", dataResp.StatusCode)
	}
	var rawBytes []byte
	rawBytes, err = io.ReadAll(dataResp.Body)
	if err != nil {
		return err
	}

	var memosize byte
	var memobytes []byte
	memosize, memobytes = memoToBinaryMemo(status.Memo)

	// App V1 sends JSON when DP or SMPC is active (needed for numeric processing),
	// otherwise pickle (the python package default).
	var srcSerializationMode enums.SerializationMode
	if status.SMPC != nil || status.DP != nil {
		srcSerializationMode = enums.SerializationJSON
	} else {
		srcSerializationMode = enums.SerializationPickle
	}
	var smpcProps models.NormalizedSMPCProperties = models.NormalizedSMPCProperties{
		Enabled: false,
	}
	var dpProps models.NormalizedDPProperties = models.NormalizedDPProperties{
		Enabled: false,
	}
	if status.SMPC != nil {
		smpcPropsRaw := models.SMPCProperties{
			Enabled:   true,
			Operation: status.SMPC.Operation,
			Exponent:  status.SMPC.Exponent,
			NumShards: status.SMPC.Shards,
		}
		smpcProps, err = smpcPropsRaw.NormalizeSmpcProperties(a.handle.Meta.FlRunBase.MaxNumberOfClients)
		if err != nil {
			return fmt.Errorf("invalid SMPC properties from app: %w", err)
		}
	}
	if status.DP != nil {
		dpPropsRaw := models.DPProperties{
			Noisetype:   status.DP.Noisetype,
			Epsilon:     status.DP.Epsilon,
			Delta:       status.DP.Delta,
			Sensitivity: status.DP.Sensitivity,
			ClippingVal: status.DP.ClippingVal,
			Enabled:     true,
		}
		dpProps, err = dpPropsRaw.NormalizeDpProperties()
		if err != nil {
			return fmt.Errorf("invalid DP properties from app: %w", err)
		}
	}

	msg := &bridge.OutgoingMessage{
		DestinationClientID:      status.Destination,
		TimestampArrived:         timeReceived,
		SrcSerializationMode:     srcSerializationMode,
		RequestedCompressionMode: enums.DefaultCompressionMode,
		MemoSize:                 memosize,
		Memo:                     memobytes,
		ToAggregatorNameSize:     byte(0), // app V1 does not specify aggregator; default to none
		ToAggregatorName:         nil,
		FromAggregatorNameSize:   byte(0), // app V1 does not specify aggregator; default to none
		FromAggregatorName:       nil,
		SmpcProps:                smpcProps,
		DpProps:                  dpProps,
		Payload:                  rawBytes,
	}

	// SafeEnqueue checks if run is active and blocks cleanup (MarkInactive)
	// from proceeding until this enqueue completes.
	if !a.handle.SafeEnqueue(msg) {
		// Run marked inactive; discard message
		return fmt.Errorf("FL run marked inactive, discarding outgoing message")
	}

	// Wake the RelayClient's outgoing watcher (non-blocking send).
	select {
	case a.handle.OutgoingNotify <- struct{}{}:
	default:
	}

	return nil
}

// postToApp POSTs one message from the relay to the app's /data endpoint.
func (a *AppCommunicatorV1) postToApp(communicationId string, fromClientId shared.ClientID, data []byte) {
	maxTries := 3
	// Do not forward auto-generated comm IDs — V1 apps don't support them.
	var memo string
	if !strings.HasPrefix(communicationId, models.AutoCommIDPrefix) {
		memo = url.QueryEscape(communicationId)
	}
	for tries := 0; tries < maxTries; tries++ {
		var targetURL string
		if len(memo) > 0 {
			targetURL = fmt.Sprintf("%s/data?client=%x&memo=%s", a.appUrl, fromClientId, memo)
		} else {
			targetURL = fmt.Sprintf("%s/data?client=%x", a.appUrl, fromClientId)
		}

		logger.Info(logV1, "", "POST to %s [Try %d/%d]", targetURL, tries+1, maxTries)

		resp, err := http.DefaultClient.Post(targetURL, "application/octet-stream", bytes.NewReader(data))
		if err != nil {
			logger.Error(logV1, "", "POST error: %v", err)
		} else {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				logger.Info(logV1, "", "Successfully relayed message from global to the app")
				return
			}
			logger.Error(logV1, "", "Failed relaying message to the app, got status code %d", resp.StatusCode)
		}

		wait := 1 << tries
		logger.Warn(logV1, "", "Error sending data from global to the app. Waiting %d second(s) before retry", wait)
		time.Sleep(time.Duration(wait) * time.Second)
	}
	logger.Error(logV1, "", "Too many failed POSTs to the app, aborting message relay to app")
}

// memoToBinaryMemo converts a string memo to its binary representation (size byte + content bytes).
func memoToBinaryMemo(memo *string) (byte, []byte) {
	if memo == nil {
		return 0, nil
	}
	memoBytes := []byte(*memo)
	if len(memoBytes) > 255 {
		logger.Error(logV1, "", "memo exceeds 255 bytes (%d), truncating", len(memoBytes))
		memoBytes = memoBytes[:255]
	}
	return byte(len(memoBytes)), memoBytes
}
