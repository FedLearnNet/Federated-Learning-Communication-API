// app [--> controller --> global relay server   (outgoing: app POSTs to HTTP server)
// app [<--] controller <-- global relay server   (incoming: app GETs from HTTP server)
// Handles all app communication for version-2 apps. Singleton, shared across all concurrent FL runs.
package appcomm

import (
	"encoding/json"
	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/codec"
	"fc_controller/pkg/controller/enums"
	"fc_controller/pkg/controller/models"
	shared_enums "fc_controller/pkg/shared/enums"
	"fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	"fc_controller/pkg/shared/util"
	"fmt"
	"github.com/fxamacker/cbor/v2"
	"io"
	"net/http"
	"sync"
	"time"
)

const LOGAPPCOMM = "APP_COMM_V2"
const READTIMEOUT = 60 * time.Second
const WRITETIMEOUT = 180 * time.Second

// AppCommunicatorV2 is a singleton HTTP server that handles app communication for V2 apps.
// It manages multiple concurrent FL runs via a map of FLRunHandles keyed by RunKey.
type AppCommunicatorV2 struct {
	port   int
	server *http.Server
	mu     sync.RWMutex
	runs   map[bridge.RunKey]*bridge.FLRunHandle
	// sendMu serializes concurrent send handlers so messages are enqueued in HTTP arrival order,
	// preventing a slower handler from enqueuing after a faster one that arrived later.
	sendMu sync.Mutex
}

// NewAppCommunicatorV2 creates a new V2 app communicator bound to the given port.
func NewAppCommunicatorV2(port int) *AppCommunicatorV2 {
	return &AppCommunicatorV2{
		port: port,
		runs: make(map[bridge.RunKey]*bridge.FLRunHandle),
	}
}

// StartServer sets up the HTTP mux and begins serving. Blocks until the server shuts down.
func (a *AppCommunicatorV2) StartServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /receive-setup", a.handleReceiveSetup)
	mux.HandleFunc("POST /send-data-to-aggregator", a.handleSendDataToAggregator)
	mux.HandleFunc("POST /send-data-to-clients", a.handleSendDataToClients)
	mux.HandleFunc("POST /receive-data-from-aggregator", a.handleReceiveDataFromAggregator)
	mux.HandleFunc("POST /receive-data-from-clients", a.handleReceiveDataFromClients)

	a.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", a.port),
		Handler:      mux,
		ReadTimeout:  READTIMEOUT,
		WriteTimeout: WRITETIMEOUT,
	}

	logger.Info(LOGAPPCOMM, "", "Starting AppCommunicatorV2 on port %d", a.port)
	if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server.
func (a *AppCommunicatorV2) Shutdown() error {
	if a.server == nil {
		return nil
	}
	return a.server.Close()
}

// AddRun registers the FLRunHandle for a new FL run, making it reachable via the HTTP endpoints.
func (a *AppCommunicatorV2) AddRun(key bridge.RunKey, handle *bridge.FLRunHandle) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs[key] = handle
}

// RemoveRun removes the FLRunHandle for a finished or stopped FL run.
// Any further app requests for this run receive 404 (not found).
func (a *AppCommunicatorV2) RemoveRun(key bridge.RunKey) {
	a.mu.Lock()
	delete(a.runs, key)
	a.mu.Unlock()
}

// lookupRun returns the FLRunHandle for the given key. If the key is not found, it writes a 404.
// If the run has failed (StateError), it writes a 410 Gone. Returns nil if an error was written.
func (a *AppCommunicatorV2) lookupRun(w http.ResponseWriter, key bridge.RunKey) *bridge.FLRunHandle {
	a.mu.RLock()
	handle, exists := a.runs[key]
	a.mu.RUnlock()
	if !exists {
		http.Error(w, "not found: no such run key", http.StatusNotFound)
		return nil
	}
	if handle.GetState() == shared_enums.StateError {
		http.Error(w, "gone: run failed", http.StatusGone) // 410
		return nil
	}
	return handle
}

// ── Response types ────────────────────────────────────────────────────────────

type ReceiveDataResponseMeta struct {
	FromClientId    string    `json:"fromClientId" cbor:"fromClientId"`
	CommunicationId string    `json:"communicationId" cbor:"communicationId"`
	TimeArrived     time.Time `json:"timeArrived" cbor:"timeArrived"`
	FromAggregator  string    `json:"fromAggregator,omitempty" cbor:"fromAggregator,omitempty"`
	ToAggregator    string    `json:"toAggregator,omitempty" cbor:"toAggregator,omitempty"`
}

type ReceiveDataResponseBodyJSON struct {
	Meta ReceiveDataResponseMeta `json:"meta"`
	Data json.RawMessage         `json:"data"`
}

type ReceiveDataResponseBodyCBOR struct {
	Meta ReceiveDataResponseMeta `cbor:"meta"`
	Data cbor.RawMessage         `cbor:"data"`
}

// ── Shared helpers ────────────────────────────────────────────────────────────

func (a *AppCommunicatorV2) getWireFormat(msg bridge.IncomingMessage) (enums.SerializationMode, enums.CompressionMode, error) {
	srcSerialization, err := enums.NormalizeSerializationModeByte(msg.SrcSerializationModeByte)
	if err != nil {
		logger.Error(LOGAPPCOMM, "", "Unknown serialization mode in message: %v, skipping", err)
		return enums.SerializationNone, enums.CompressionNone, fmt.Errorf("unknown serialization mode: %v", err)
	}
	srcCompression, err := enums.NormalizeCompressionModeByte(msg.SrcCompressionModeByte)
	if err != nil {
		logger.Error(LOGAPPCOMM, "", "Unknown compression mode in message: %v, skipping", err)
		return enums.SerializationNone, enums.CompressionNone, fmt.Errorf("unknown compression mode: %v", err)
	}
	return srcSerialization, srcCompression, nil
}

func (a *AppCommunicatorV2) toJSONResponseBody(msg bridge.IncomingMessage, serializationFormat enums.SerializationMode) (ReceiveDataResponseBodyJSON, error) {
	srcSerialization, srcCompression, err := a.getWireFormat(msg)
	if err != nil {
		return ReceiveDataResponseBodyJSON{}, err
	}
	transcodedData, err := codec.DecodeEncode(msg.Data, nil, srcSerialization, srcCompression, nil, serializationFormat, enums.CompressionNone)
	if err != nil {
		return ReceiveDataResponseBodyJSON{}, fmt.Errorf("transcode failed: %w", err)
	}
	return ReceiveDataResponseBodyJSON{
		Meta: ReceiveDataResponseMeta{
			FromClientId:    msg.FromClientId.ToString(),
			CommunicationId: msg.CommunicationId,
			TimeArrived:     msg.TimestampArrived,
			FromAggregator:  msg.FromAggregatorName,
			ToAggregator:    msg.ToAggregatorName,
		},
		Data: json.RawMessage(transcodedData),
	}, nil
}

func (a *AppCommunicatorV2) toCBORResponseBody(msg bridge.IncomingMessage) (ReceiveDataResponseBodyCBOR, error) {
	srcSerialization, srcCompression, err := a.getWireFormat(msg)
	if err != nil {
		return ReceiveDataResponseBodyCBOR{}, err
	}
	transcodedData, err := codec.DecodeEncode(msg.Data, nil, srcSerialization, srcCompression, nil, enums.SerializationCBOR, enums.CompressionNone)
	if err != nil {
		return ReceiveDataResponseBodyCBOR{}, fmt.Errorf("transcode failed: %w", err)
	}
	return ReceiveDataResponseBodyCBOR{
		Meta: ReceiveDataResponseMeta{
			FromClientId:    msg.FromClientId.ToString(),
			CommunicationId: msg.CommunicationId,
			TimeArrived:     msg.TimestampArrived,
			FromAggregator:  msg.FromAggregatorName,
			ToAggregator:    msg.ToAggregatorName,
		},
		Data: cbor.RawMessage(transcodedData),
	}, nil
}

func (a *AppCommunicatorV2) getJSONMessages(messages []bridge.IncomingMessage) []ReceiveDataResponseBodyJSON {
	responses := make([]ReceiveDataResponseBodyJSON, 0, len(messages))
	for i, msg := range messages {
		body, err := a.toJSONResponseBody(msg, enums.SerializationJSON)
		if err != nil {
			logger.Error(LOGAPPCOMM, "", "Failed to transcode message %d to JSON: %v, skipping", i, err)
			continue
		}
		responses = append(responses, body)
	}
	return responses
}

func (a *AppCommunicatorV2) getCBORMessages(messages []bridge.IncomingMessage) []ReceiveDataResponseBodyCBOR {
	responses := make([]ReceiveDataResponseBodyCBOR, 0, len(messages))
	for i, msg := range messages {
		body, err := a.toCBORResponseBody(msg)
		if err != nil {
			logger.Error(LOGAPPCOMM, "", "Failed to transcode message %d to CBOR: %v, skipping", i, err)
			continue
		}
		responses = append(responses, body)
	}
	return responses
}

func (a *AppCommunicatorV2) getJSONGroupedMessages(grouped map[string][]bridge.IncomingMessage) map[string][]ReceiveDataResponseBodyJSON {
	result := make(map[string][]ReceiveDataResponseBodyJSON, len(grouped))
	for commID, messages := range grouped {
		result[commID] = a.getJSONMessages(messages)
	}
	return result
}

func (a *AppCommunicatorV2) getCBORGroupedMessages(grouped map[string][]bridge.IncomingMessage) map[string][]ReceiveDataResponseBodyCBOR {
	result := make(map[string][]ReceiveDataResponseBodyCBOR, len(grouped))
	for commID, messages := range grouped {
		result[commID] = a.getCBORMessages(messages)
	}
	return result
}

func (a *AppCommunicatorV2) getAggregatorBytes(aggregator *string) ([]byte, byte, error) {
	if aggregator == nil || len(*aggregator) == 0 {
		return nil, 0, nil
	}
	aggregatorBytes := []byte(*aggregator)
	if len(aggregatorBytes) > 255 {
		return nil, 0, fmt.Errorf("aggregator name too long (max 255 bytes)")
	}
	return aggregatorBytes, byte(len(aggregatorBytes)), nil
}

// readMultipartRequest parses a multipart/form-data request, extracting the "metadata" JSON part
// and the "data" binary part. Returns an error message suitable for http.Error if parsing fails.
func readMultipartRequest(r *http.Request) (metadataJSON []byte, data []byte, errMsg string) {
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, nil, "bad request: failed to read multipart form"
	}
	metadataFound := false
	dataFound := false
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, "bad request: error reading multipart part"
		}
		defer part.Close()
		switch part.FormName() {
		case "metadata":
			metadataJSON, err = io.ReadAll(part)
			if err != nil {
				return nil, nil, "bad request: failed to read metadata part"
			}
			metadataFound = true
		case "data":
			data, err = io.ReadAll(part)
			if err != nil {
				return nil, nil, "bad request: failed to read data part"
			}
			dataFound = true
		}
	}
	if !metadataFound {
		return nil, nil, "bad request: missing metadata part"
	}
	if !dataFound {
		return nil, nil, "bad request: missing data part"
	}
	return metadataJSON, data, ""
}

// writeJSONResponse encodes v as JSON and writes it to w with Content-Type application/json.
func writeJSONResponse(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Error(LOGAPPCOMM, "", "Failed to encode JSON response: %v", err)
	}
}

// writeCBORResponse encodes v as CBOR and writes it to w with Content-Type application/cbor.
func writeCBORResponse(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/cbor")
	w.WriteHeader(http.StatusOK)
	if err := cbor.NewEncoder(w).Encode(v); err != nil {
		logger.Error(LOGAPPCOMM, "", "Failed to encode CBOR response: %v", err)
	}
}

// ── /receive-setup ────────────────────────────────────────────────────────────

type ReceiveSetupRequestBody struct {
	AppKey  util.APIKey       `json:"appKey"`
	Channel link.RelayChannel `json:"channel"`
}

type ReceiveSetupResponseBody struct {
	AggregatorId  string   `json:"aggregatorId"`  // coordinator ID
	ClientOrder   []string `json:"clientOrder"`   // ordered list of client IDs
	MaxNumClients int      `json:"maxNumClients"` // max clients for this run
}

func (a *AppCommunicatorV2) handleReceiveSetup(w http.ResponseWriter, r *http.Request) {
	body := ReceiveSetupRequestBody{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request: invalid JSON", http.StatusBadRequest)
		return
	}
	key := bridge.RunKey{Channel: body.Channel, AppKey: body.AppKey}

	handle := a.lookupRun(w, key)
	if handle == nil {
		return
	}

	clientOrder := make([]string, len(handle.Meta.ClientIDOrder))
	for i, clientID := range handle.Meta.ClientIDOrder {
		clientOrder[i] = clientID.ToString()
	}
	writeJSONResponse(w, ReceiveSetupResponseBody{
		AggregatorId:  handle.Meta.CoordinatorID.ToString(),
		ClientOrder:   clientOrder,
		MaxNumClients: handle.Meta.MaxNumberOfClients,
	})
	logger.Debug(LOGAPPCOMM, "", "Returned setup info for channel %s: %d known clients", key.Channel.ToString(), len(clientOrder))
}

// ── /send-data-to-aggregator ──────────────────────────────────────────────────

// SendDataToAggregatorMetadata is the JSON metadata part for POST /send-data-to-aggregator.
type SendDataToAggregatorMetadata struct {
	AppKey            util.APIKey             `json:"appKey"`
	Channel           link.RelayChannel       `json:"channel"`
	SerializationUsed enums.SerializationMode `json:"serializationUsed"`
	To                []link.ClientID         `json:"to,omitempty"`              // empty = send to aggregator (ZERO_CLIENT_ID)
	ToAggregator      string                  `json:"toAggregator"`              // required: which aggregator receives the message
	CommunicationId   *string                 `json:"communicationId,omitempty"` // nil/"" /"#AUTOMATIC" → automatic comm ID
	CompressionToUse  *enums.CompressionMode  `json:"compressionToUse,omitempty"`
	SMPCProperties    *models.SMPCProperties  `json:"smpc,omitempty"`
	DPProperties      *models.DPProperties    `json:"dp,omitempty"`
}

// handleSendDataToAggregator handles POST /send-data-to-aggregator.
// Called by a CLIENT to push a message toward an aggregator on the coordinator side.
//
// Request: multipart/form-data
//   - "metadata" (JSON): SendDataToAggregatorMetadata
//   - "data" (binary): serialized payload
//
// Comm ID rules:
//   - nil, "", or "#AUTOMATIC" → automatic comm ID (MemoSize=0; auto-ID assigned by relay client)
//   - any other string → manual comm ID; duplicate re-use → 409 + run fails
//
// Errors: 400, 404, 409 (run stopped or duplicate comm ID), 410 (run failed)
func (a *AppCommunicatorV2) handleSendDataToAggregator(w http.ResponseWriter, r *http.Request) {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	timeReceived := time.Now()

	metadataJSON, data, errMsg := readMultipartRequest(r)
	if errMsg != "" {
		http.Error(w, errMsg, http.StatusBadRequest)
		logger.Error(LOGAPPCOMM, "", "handleSendDataToAggregator: %s", errMsg)
		return
	}

	var metadata SendDataToAggregatorMetadata
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		http.Error(w, "bad request: invalid metadata JSON", http.StatusBadRequest)
		logger.Error(LOGAPPCOMM, "", "handleSendDataToAggregator: failed to decode metadata: %v", err)
		return
	}
	if metadata.ToAggregator == "" {
		http.Error(w, "bad request: toAggregator is required", http.StatusBadRequest)
		return
	}

	key := bridge.RunKey{Channel: metadata.Channel, AppKey: metadata.AppKey}
	handle := a.lookupRun(w, key)
	if handle == nil {
		return
	}
	if handle.Meta.IsCoordinator {
		http.Error(w, "bad request: only clients can send to aggregator", http.StatusBadRequest)
		return
	}

	// Normalize comm ID: nil/"" /"#AUTOMATIC" → memo=nil so MemoSize=0, triggering auto-ID generation.
	var memo []byte
	if metadata.CommunicationId != nil &&
		*metadata.CommunicationId != "" &&
		*metadata.CommunicationId != models.RequestAutoCommId {
		memo = []byte(*metadata.CommunicationId)
	} else {
		memo = nil
	}

	toAggBytes, toAggSize, err := a.getAggregatorBytes(&metadata.ToAggregator)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad request: invalid toAggregator: %v", err), http.StatusBadRequest)
		return
	}

	compressionToUse := enums.DefaultCompressionMode
	if metadata.CompressionToUse != nil {
		compressionToUse = *metadata.CompressionToUse
	}

	var normalizedSmpc models.NormalizedSMPCProperties
	if metadata.SMPCProperties == nil {
		normalizedSmpc = models.NormalizedSMPCProperties{Enabled: false}
	} else {
		normalizedSmpc, err = metadata.SMPCProperties.NormalizeSmpcProperties(handle.Meta.CountKnownClients())
		if err != nil {
			http.Error(w, "bad request: invalid SMPC properties", http.StatusBadRequest)
			logger.Error(LOGAPPCOMM, "", "NormalizeSmpcProperties failed: %v", err)
			return
		}
	}
	var normalizedDp models.NormalizedDPProperties
	if metadata.DPProperties == nil {
		normalizedDp = models.NormalizedDPProperties{Enabled: false}
	} else {
		normalizedDp, err = metadata.DPProperties.NormalizeDpProperties()
		if err != nil {
			http.Error(w, "bad request: invalid DP properties", http.StatusBadRequest)
			logger.Error(LOGAPPCOMM, "", "NormalizeDpProperties failed: %v", err)
			return
		}
	}

	destinations := destinationsFromTo(metadata.To)
	enqueueCount := 0
	for _, destID := range destinations {
		outMsg := &bridge.OutgoingMessage{
			DestinationClientID:      destID,
			TimestampArrived:         timeReceived,
			SrcSerializationMode:     metadata.SerializationUsed,
			RequestedCompressionMode: compressionToUse,
			Payload:                  data,
			SmpcProps:                normalizedSmpc,
			DpProps:                  normalizedDp,
			MemoSize:                 byte(len(memo)),
			Memo:                     memo,
			ToAggregatorNameSize:     toAggSize,
			ToAggregatorName:         toAggBytes,
		}
		if !handle.SafeEnqueue(outMsg) {
			http.Error(w, "conflict: run stopped or duplicate comm ID", http.StatusConflict)
			logger.Warn(LOGAPPCOMM, "", "SafeEnqueue failed for key: %v", key)
			return
		}
		logger.Debug(LOGAPPCOMM, "", "Enqueued outgoing message with memo=%s to aggregator %s for channel %s", string(memo), metadata.ToAggregator, key.Channel.ToString())
		enqueueCount++
	}
	signalOutgoing(handle)
	logger.Debug(LOGAPPCOMM, "", "Enqueued %d message(s) to aggregator for channel %s", enqueueCount, key.Channel.ToString())
	w.WriteHeader(http.StatusOK)
}

// ── /send-data-to-clients ─────────────────────────────────────────────────────

// SendDataToClientsMetadata is the JSON metadata part for POST /send-data-to-clients.
type SendDataToClientsMetadata struct {
	AppKey            util.APIKey             `json:"appKey"`
	Channel           link.RelayChannel       `json:"channel"`
	SerializationUsed enums.SerializationMode `json:"serializationUsed"`
	To                []link.ClientID         `json:"to,omitempty"`             // empty = broadcast to all clients
	FromAggregator    *string                 `json:"fromAggregator,omitempty"` // optional: identifies which aggregator is sending
	CommunicationId   string                  `json:"communicationId"`          // REQUIRED — always explicit, no auto comm ID
	CompressionToUse  *enums.CompressionMode  `json:"compressionToUse,omitempty"`
	DPProperties      *models.DPProperties    `json:"dp,omitempty"`
	// Note: SMPC is not supported on this path — client-bound messages cannot be aggregated.
}

// handleSendDataToClients handles POST /send-data-to-clients.
// Called by ANY participant (coordinator or client) to send a message to one or more clients.
// Covers coordinator→client broadcasts and client↔client P2P messages.
//
// Request: multipart/form-data
//   - "metadata" (JSON): SendDataToClientsMetadata
//   - "data" (binary): serialized payload
//
// Comm ID rules:
//   - REQUIRED: must be a non-empty, non-"#AUTOMATIC" string; otherwise 400.
//   - When responding as an aggregator, the coordinator MUST echo the specific auto comm ID
//     it received from clients (e.g., "AUTOMATIC_COMM_ID_1") so clients can identify the round.
//   - Duplicate detection is keyed on (commID, fromAggregator) so the same round's comm ID
//     can be reused across different aggregators without triggering a false positive.
//
// Errors: 400, 404, 409 (run stopped or duplicate comm ID), 410 (run failed)
func (a *AppCommunicatorV2) handleSendDataToClients(w http.ResponseWriter, r *http.Request) {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	timeReceived := time.Now()

	metadataJSON, data, errMsg := readMultipartRequest(r)
	if errMsg != "" {
		http.Error(w, errMsg, http.StatusBadRequest)
		logger.Error(LOGAPPCOMM, "", "handleSendDataToClients: %s", errMsg)
		return
	}

	var metadata SendDataToClientsMetadata
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		http.Error(w, "bad request: invalid metadata JSON", http.StatusBadRequest)
		logger.Error(LOGAPPCOMM, "", "handleSendDataToClients: failed to decode metadata: %v", err)
		return
	}
	if metadata.CommunicationId == "" || metadata.CommunicationId == models.RequestAutoCommId {
		http.Error(w, "bad request: communicationId is required and must not be empty or \"#AUTOMATIC\"", http.StatusBadRequest)
		return
	}

	key := bridge.RunKey{Channel: metadata.Channel, AppKey: metadata.AppKey}
	handle := a.lookupRun(w, key)
	if handle == nil {
		return
	}

	for _, id := range metadata.To {
		if handle.Meta.IsCoordinatorID(id) {
			http.Error(w, "bad request: cannot send peer-to-peer message to the aggregator", http.StatusBadRequest)
			return
		}
	}

	fromAggBytes, fromAggSize, err := a.getAggregatorBytes(metadata.FromAggregator)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad request: invalid fromAggregator: %v", err), http.StatusBadRequest)
		return
	}

	compressionToUse := enums.DefaultCompressionMode
	if metadata.CompressionToUse != nil {
		compressionToUse = *metadata.CompressionToUse
	}

	var normalizedDp models.NormalizedDPProperties
	if metadata.DPProperties == nil {
		normalizedDp = models.NormalizedDPProperties{Enabled: false}
	} else {
		normalizedDp, err = metadata.DPProperties.NormalizeDpProperties()
		if err != nil {
			http.Error(w, "bad request: invalid DP properties", http.StatusBadRequest)
			logger.Error(LOGAPPCOMM, "", "NormalizeDpProperties failed: %v", err)
			return
		}
	}

	memo := []byte(metadata.CommunicationId)
	destinations := destinationsFromTo(metadata.To)
	enqueueCount := 0
	for _, destID := range destinations {
		outMsg := &bridge.OutgoingMessage{
			DestinationClientID:      destID,
			TimestampArrived:         timeReceived,
			SrcSerializationMode:     metadata.SerializationUsed,
			RequestedCompressionMode: compressionToUse,
			Payload:                  data,
			SmpcProps:                models.NormalizedSMPCProperties{Enabled: false},
			DpProps:                  normalizedDp,
			MemoSize:                 byte(len(memo)),
			Memo:                     memo,
			FromAggregatorNameSize:   fromAggSize,
			FromAggregatorName:       fromAggBytes,
		}
		if !handle.SafeEnqueue(outMsg) {
			http.Error(w, "conflict: run stopped or duplicate comm ID", http.StatusConflict)
			logger.Warn(LOGAPPCOMM, "", "SafeEnqueue failed for key: %v", key)
			return
		}
		enqueueCount++
	}
	signalOutgoing(handle)
	logger.Debug(LOGAPPCOMM, "", "Enqueued %d message(s) to clients for channel %s", enqueueCount, key.Channel.ToString())
	w.WriteHeader(http.StatusOK)
}

// ── /receive-data-from-aggregator ─────────────────────────────────────────────

// ReceiveDataFromAggregatorRequestBody is the JSON body for POST /receive-data-from-aggregator.
type ReceiveDataFromAggregatorRequestBody struct {
	AppKey              util.APIKey             `json:"appKey"`
	Channel             link.RelayChannel       `json:"channel"`
	SerializationFormat enums.SerializationMode `json:"serializationFormat"`
	FromAggregator      string                  `json:"fromAggregator"`            // required
	CommunicationId     *string                 `json:"communicationId,omitempty"` // nil/"" /"#AUTOMATIC" → newest auto
}

// handleReceiveDataFromAggregator handles POST /receive-data-from-aggregator.
// Called by a CLIENT to pull the latest message sent by an aggregator.
//
// Request (JSON body): ReceiveDataFromAggregatorRequestBody
//
// Comm ID rules:
//   - nil, "", or "#AUTOMATIC" → returns the SINGLE NEWEST auto-comm-ID message (highest
//     counter value). All older auto messages for this aggregator are discarded, ensuring
//     the client is always in the most up-to-date communication round.
//   - any other string → returns the message with that specific comm ID
//
// Response: single ReceiveDataResponseBody (not an array); 204 No Content if no match.
//
// Errors: 400, 404, 409 (run stopped), 410 (run failed)
func (a *AppCommunicatorV2) handleReceiveDataFromAggregator(w http.ResponseWriter, r *http.Request) {
	var body ReceiveDataFromAggregatorRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request: invalid JSON", http.StatusBadRequest)
		return
	}
	if body.FromAggregator == "" {
		http.Error(w, "bad request: fromAggregator is required", http.StatusBadRequest)
		return
	}
	if body.SerializationFormat != enums.SerializationJSON && body.SerializationFormat != enums.SerializationCBOR {
		http.Error(w, "bad request: unsupported serializationFormat", http.StatusBadRequest)
		return
	}

	key := bridge.RunKey{Channel: body.Channel, AppKey: body.AppKey}
	handle := a.lookupRun(w, key)
	if handle == nil {
		return
	}
	if handle.Meta.IsCoordinator {
		http.Error(w, "bad request: only clients can receive from aggregator", http.StatusBadRequest)
		return
	}

	isAuto := body.CommunicationId == nil || *body.CommunicationId == "" || *body.CommunicationId == models.RequestAutoCommId

	var msg *bridge.IncomingMessage
	if isAuto {
		fromAgg := body.FromAggregator
		msg = handle.SafePullNewestAuto(&fromAgg, nil)
	} else {
		fromAgg := body.FromAggregator
		messages := handle.SafePull(body.CommunicationId, nil, &fromAgg, nil)
		if messages == nil {
			http.Error(w, "conflict: run stopped", http.StatusConflict)
			return
		}
		if len(messages) > 0 {
			msg = &messages[0]
			if len(messages) > 1 {
				logger.Warn(LOGAPPCOMM, "", "Expected 1 message from aggregator for comm ID %q, got %d", *body.CommunicationId, len(messages))
			}
		}
	}

	if msg == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if body.SerializationFormat == enums.SerializationJSON {
		responseBody, err := a.toJSONResponseBody(*msg, enums.SerializationJSON)
		if err != nil {
			http.Error(w, "internal server error: failed to transcode message", http.StatusInternalServerError)
			logger.Error(LOGAPPCOMM, "", "handleReceiveDataFromAggregator transcode failed: %v", err)
			return
		}
		writeJSONResponse(w, responseBody)
	} else {
		responseBody, err := a.toCBORResponseBody(*msg)
		if err != nil {
			http.Error(w, "internal server error: failed to transcode message", http.StatusInternalServerError)
			logger.Error(LOGAPPCOMM, "", "handleReceiveDataFromAggregator CBOR transcode failed: %v", err)
			return
		}
		writeCBORResponse(w, responseBody)
	}
	logger.Debug(LOGAPPCOMM, "", "Returned message from aggregator for channel %s (commID=%q)", key.Channel.ToString(), msg.CommunicationId)
}

// ── /receive-data-from-clients ────────────────────────────────────────────────

// ReceiveDataFromClientsRequestBody is the JSON body for POST /receive-data-from-clients.
type ReceiveDataFromClientsRequestBody struct {
	AppKey              util.APIKey             `json:"appKey"`
	Channel             link.RelayChannel       `json:"channel"`
	SerializationFormat enums.SerializationMode `json:"serializationFormat"`
	ToAggregator        *string                 `json:"toAggregator,omitempty"`    // optional: filter by aggregator
	CommunicationId     *string                 `json:"communicationId,omitempty"` // nil/"" → all; "#AUTOMATIC" → auto only; else manual
	FromClientIds       []link.ClientID         `json:"fromClientIds,omitempty"`   // empty = all clients
	MinPackages         *int                    `json:"minPackages"`               // required: min messages per comm ID group
}

// handleReceiveDataFromClients handles POST /receive-data-from-clients.
// Called by the COORDINATOR (to aggregate client messages) or any CLIENT (P2P receive).
//
// Request (JSON body): ReceiveDataFromClientsRequestBody
//
// Comm ID rules:
//   - nil or ""    → all comm IDs (manual and auto); full store scan respecting other filters
//   - "#AUTOMATIC" → auto-comm-ID messages only
//   - any other    → that specific comm ID; response map has at most one key
//
// Response: always map[commID][]ReceiveDataResponseBody. Only groups with ≥ minPackages
// messages are included. An empty map means no complete groups exist yet (poll again).
//
// Errors: 400 (missing/invalid minPackages), 404, 409 (run stopped), 410 (run failed)
func (a *AppCommunicatorV2) handleReceiveDataFromClients(w http.ResponseWriter, r *http.Request) {
	var body ReceiveDataFromClientsRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request: invalid JSON", http.StatusBadRequest)
		return
	}
	if body.MinPackages == nil || *body.MinPackages <= 0 {
		http.Error(w, "bad request: minPackages is required and must be > 0", http.StatusBadRequest)
		return
	}
	if body.SerializationFormat != enums.SerializationJSON && body.SerializationFormat != enums.SerializationCBOR {
		http.Error(w, "bad request: unsupported serializationFormat", http.StatusBadRequest)
		return
	}

	key := bridge.RunKey{Channel: body.Channel, AppKey: body.AppKey}
	handle := a.lookupRun(w, key)
	if handle == nil {
		return
	}

	grouped := handle.SafePullGroupedByCommID(*body.MinPackages, body.CommunicationId, body.ToAggregator, body.FromClientIds)
	if grouped == nil {
		http.Error(w, "conflict: run stopped", http.StatusConflict)
		return
	}

	if body.SerializationFormat == enums.SerializationJSON {
		writeJSONResponse(w, a.getJSONGroupedMessages(grouped))
		logger.Debug(LOGAPPCOMM, "", "Returned %d comm ID group(s) from clients for channel %s (JSON)", len(grouped), key.Channel.ToString())
	} else {
		writeCBORResponse(w, a.getCBORGroupedMessages(grouped))
		logger.Debug(LOGAPPCOMM, "", "Returned %d comm ID group(s) from clients for channel %s (CBOR)", len(grouped), key.Channel.ToString())
	}
}

// ── Private helpers ───────────────────────────────────────────────────────────

// destinationsFromTo converts a To list into destination ClientID pointers.
// An empty list maps to ZERO_CLIENT_ID (broadcast / send-to-aggregator convention).
func destinationsFromTo(to []link.ClientID) []*link.ClientID {
	if len(to) == 0 {
		return []*link.ClientID{&link.ZERO_CLIENT_ID}
	}
	dests := make([]*link.ClientID, len(to))
	for i := range to {
		dests[i] = &to[i]
	}
	return dests
}

// signalOutgoing sends a non-blocking signal on handle.OutgoingNotify to wake the RelayClient.
func signalOutgoing(handle *bridge.FLRunHandle) {
	select {
	case handle.OutgoingNotify <- struct{}{}:
	default:
	}
}
