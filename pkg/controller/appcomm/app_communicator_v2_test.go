package appcomm

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/enums"
	"fc_controller/pkg/controller/models"
	shared_enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
)

// ── Test fixtures ─────────────────────────────────────────────────────────────

func newV2TestHandle(t *testing.T, isCoordinator bool) (*bridge.FLRunHandle, bridge.RunKey) {
	t.Helper()
	privKey, pubKey, err := util.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	channel := shared.RelayChannel{}
	channel[0] = 1

	coordID := shared.ClientID{}
	coordID[0] = 1
	clientID1 := shared.ClientID{}
	clientID1[0] = 2
	clientID2 := shared.ClientID{}
	clientID2[0] = 3

	ownID := clientID1
	if isCoordinator {
		ownID = coordID
	}
	// For v2 apps, the coordinator is separate from the clients and must NOT appear in clientOrder.
	clientOrder := []shared.ClientID{clientID1, clientID2}

	meta, err := bridge.NewFlRunMeta(
		channel, "test-app-key", "relay-key",
		coordID, ownID, isCoordinator,
		len(clientOrder), clientOrder,
		pubKey, privKey,
		shared_enums.AppVersionV2,
	)
	if err != nil {
		t.Fatalf("NewFlRunMeta: %v", err)
	}
	handle := bridge.NewFLRunHandle(meta)
	key := bridge.RunKey{Channel: channel, AppKey: "test-app-key"}
	return handle, key
}

func newV2TestApp(t *testing.T, isCoordinator bool) (*AppCommunicatorV2, *bridge.FLRunHandle, bridge.RunKey) {
	t.Helper()
	handle, key := newV2TestHandle(t, isCoordinator)
	app := NewAppCommunicatorV2(0)
	app.AddRun(key, handle)
	return app, handle, key
}

// buildMultipartReq builds a multipart/form-data POST request with a "metadata" JSON part
// and a "data" binary part.
func buildMultipartReq(t *testing.T, target string, metadata any, data []byte) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	mp, err := mw.CreateFormField("metadata")
	if err != nil {
		t.Fatalf("create metadata part: %v", err)
	}
	mp.Write(metaJSON) //nolint:errcheck

	dp, err := mw.CreateFormField("data")
	if err != nil {
		t.Fatalf("create data part: %v", err)
	}
	dp.Write(data) //nolint:errcheck

	mw.Close()
	req := httptest.NewRequest("POST", target, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

// buildJSONReq builds a JSON-body POST request.
func buildJSONReq(t *testing.T, target string, body any) *http.Request {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest("POST", target, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// storeIncomingAutoMsg stores an auto-comm-ID message directly in the handle's Incoming store,
// bypassing SafeIncomingStore so tests can pre-populate without triggering duplicate detection.
func storeIncomingAutoMsg(handle *bridge.FLRunHandle, counter string, fromAgg string, clientID shared.ClientID) {
	handle.Incoming.Store(bridge.IncomingMessage{
		CommunicationId:          models.AutoCommIDPrefix + counter,
		FromClientId:             clientID,
		FromAggregatorName:       fromAgg,
		SrcSerializationModeByte: enums.SerializationJSONByte,
		SrcCompressionModeByte:   enums.CompressionNoneByte,
		TimestampArrived:         time.Now(),
		Data:                     []byte(`{"counter":` + counter + `}`),
	})
}

// strPtr returns a pointer to s.
func strPtr2(s string) *string { return &s }

// ── handleSendDataToAggregator ────────────────────────────────────────────────

// TestSendToAggregator_AutoCommIDs_AssignedAtEnqueue verifies that nil, "", and "#AUTOMATIC"
// comm IDs all cause an automatic comm ID to be assigned during SafeEnqueue (MemoSize > 0,
// starts with AutoCommIDPrefix).
func TestSendToAggregator_AutoCommIDs_AssignedAtEnqueue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commID *string
	}{
		{"nil", nil},
		{"empty", strPtr2("")},
		{"#AUTOMATIC", strPtr2(models.RequestAutoCommId)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, handle, key := newV2TestApp(t, false) // client

			metadata := SendDataToAggregatorMetadata{
				AppKey:            key.AppKey,
				Channel:           key.Channel,
				SerializationUsed: enums.SerializationJSON,
				ToAggregator:      "agg1",
				CommunicationId:   tc.commID,
			}
			req := buildMultipartReq(t, "/send-data-to-aggregator", metadata, []byte(`"payload"`))
			w := httptest.NewRecorder()
			app.handleSendDataToAggregator(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
			}
			msg, ok := handle.Outgoing.Dequeue()
			if !ok {
				t.Fatal("expected message in outgoing queue")
			}
			if msg.MemoSize == 0 {
				t.Error("expected auto comm ID to be assigned at enqueue time (MemoSize > 0)")
			}
			commId := string(msg.Memo[:msg.MemoSize])
			if !strings.HasPrefix(commId, models.AutoCommIDPrefix) {
				t.Errorf("expected auto comm ID prefix %q, got %q", models.AutoCommIDPrefix, commId)
			}
		})
	}
}

// TestSendToAggregator_ManualCommID_MemoSizeNonZero verifies that a manual comm ID is stored
// in the outgoing message's Memo field with the correct size.
func TestSendToAggregator_ManualCommID_MemoSizeNonZero(t *testing.T) {
	app, handle, key := newV2TestApp(t, false)

	commID := "round-42"
	metadata := SendDataToAggregatorMetadata{
		AppKey:            key.AppKey,
		Channel:           key.Channel,
		SerializationUsed: enums.SerializationJSON,
		ToAggregator:      "agg1",
		CommunicationId:   &commID,
	}
	req := buildMultipartReq(t, "/send-data-to-aggregator", metadata, []byte(`"payload"`))
	w := httptest.NewRecorder()
	app.handleSendDataToAggregator(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	msg, ok := handle.Outgoing.Dequeue()
	if !ok {
		t.Fatal("expected message in outgoing queue")
	}
	if msg.MemoSize != byte(len(commID)) {
		t.Errorf("expected MemoSize=%d, got %d", len(commID), msg.MemoSize)
	}
	if string(msg.Memo[:msg.MemoSize]) != commID {
		t.Errorf("expected Memo=%q, got %q", commID, string(msg.Memo[:msg.MemoSize]))
	}
}

// TestSendToAggregator_DuplicateCommID verifies that a duplicate manual comm ID returns 409,
// asynchronously transitions the run to StateError, and subsequent requests receive 410.
func TestSendToAggregator_DuplicateCommID_409ThenRunFails410(t *testing.T) {
	app, handle, key := newV2TestApp(t, false)

	commID := "dup-id"
	send := func() int {
		metadata := SendDataToAggregatorMetadata{
			AppKey:            key.AppKey,
			Channel:           key.Channel,
			SerializationUsed: enums.SerializationJSON,
			ToAggregator:      "agg1",
			CommunicationId:   &commID,
		}
		req := buildMultipartReq(t, "/send-data-to-aggregator", metadata, []byte(`"payload"`))
		w := httptest.NewRecorder()
		app.handleSendDataToAggregator(w, req)
		return w.Code
	}

	if code := send(); code != http.StatusOK {
		t.Fatalf("first send: expected 200, got %d", code)
	}
	if code := send(); code != http.StatusConflict {
		t.Fatalf("duplicate send: expected 409, got %d", code)
	}

	// Wait for the async MarkError goroutine to transition the run to StateError.
	deadline := time.Now().Add(200 * time.Millisecond)
	for handle.GetState() != shared_enums.StateError && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if handle.GetState() != shared_enums.StateError {
		t.Fatal("run did not transition to StateError after duplicate comm ID")
	}

	if code := send(); code != http.StatusGone {
		t.Fatalf("post-error send: expected 410 Gone, got %d", code)
	}
}

// TestSendToAggregator_CoordinatorRejected verifies that coordinators cannot call
// /send-data-to-aggregator (client-only endpoint).
func TestSendToAggregator_CoordinatorRejected(t *testing.T) {
	app, _, key := newV2TestApp(t, true) // coordinator

	commID := "comm-1"
	metadata := SendDataToAggregatorMetadata{
		AppKey:            key.AppKey,
		Channel:           key.Channel,
		SerializationUsed: enums.SerializationJSON,
		ToAggregator:      "agg1",
		CommunicationId:   &commID,
	}
	req := buildMultipartReq(t, "/send-data-to-aggregator", metadata, []byte(`"payload"`))
	w := httptest.NewRecorder()
	app.handleSendDataToAggregator(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for coordinator, got %d", w.Code)
	}
}

// ── handleSendDataToClients ───────────────────────────────────────────────────

// TestSendToClients_EmptyOrAutoCommID_Returns400 verifies that missing or auto comm IDs
// are rejected — /send-data-to-clients always requires an explicit comm ID.
func TestSendToClients_EmptyOrAutoCommID_Returns400(t *testing.T) {
	for _, commID := range []string{"", models.RequestAutoCommId} {
		t.Run(commID, func(t *testing.T) {
			app, _, key := newV2TestApp(t, true)
			metadata := SendDataToClientsMetadata{
				AppKey:            key.AppKey,
				Channel:           key.Channel,
				SerializationUsed: enums.SerializationJSON,
				CommunicationId:   commID,
			}
			req := buildMultipartReq(t, "/send-data-to-clients", metadata, []byte(`"payload"`))
			w := httptest.NewRecorder()
			app.handleSendDataToClients(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("commID=%q: expected 400, got %d", commID, w.Code)
			}
		})
	}
}

// TestSendToClients_ManualCommID_Returns200 verifies a valid send-to-clients request succeeds,
// including the coordinator echoing an auto comm ID string as the round identifier.
// The coordinator must have received client data for that round first (recvCounter >= counter).
func TestSendToClients_ManualCommID_Returns200(t *testing.T) {
	app, handle, key := newV2TestApp(t, true) // coordinator

	// Simulate a client sending AUTOMATIC_COMM_ID_1 to aggregator "agg1" so the coordinator's
	// recvCounter["agg1"] advances to 1 before it tries to broadcast AUTOMATIC_COMM_ID_1.
	clientID := shared.ClientID{}
	clientID[0] = 2
	handle.SafeIncomingStore(bridge.IncomingMessage{
		CommunicationId:          models.AutoCommIDPrefix + "1",
		FromClientId:             clientID,
		ToAggregatorName:         "agg1",
		SrcSerializationModeByte: enums.SerializationJSONByte,
		SrcCompressionModeByte:   enums.CompressionNoneByte,
		TimestampArrived:         time.Now(),
	})

	fromAgg := "agg1"
	metadata := SendDataToClientsMetadata{
		AppKey:            key.AppKey,
		Channel:           key.Channel,
		SerializationUsed: enums.SerializationJSON,
		CommunicationId:   models.AutoCommIDPrefix + "1", // echo the round's auto comm ID
		FromAggregator:    &fromAgg,
	}
	req := buildMultipartReq(t, "/send-data-to-clients", metadata, []byte(`"payload"`))
	w := httptest.NewRecorder()
	app.handleSendDataToClients(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if handle.Outgoing.Len() == 0 {
		t.Error("expected message enqueued in outgoing queue")
	}
}

// ── handleReceiveDataFromAggregator ──────────────────────────────────────────

// TestReceiveFromAggregator_AutoCommID_ReturnsNewestDiscardingOlders verifies that when a client
// requests with an auto comm ID (empty/nil/#AUTOMATIC), only the message with the highest counter
// is returned, and all older auto messages for that aggregator are discarded.
func TestReceiveFromAggregator_AutoCommID_ReturnsNewestDiscardingOlders(t *testing.T) {
	app, handle, key := newV2TestApp(t, false) // client

	clientID := shared.ClientID{}
	clientID[0] = 1

	storeIncomingAutoMsg(handle, "1", "agg1", clientID)
	storeIncomingAutoMsg(handle, "9", "agg1", clientID)
	storeIncomingAutoMsg(handle, "10", "agg1", clientID)

	body := ReceiveDataFromAggregatorRequestBody{
		AppKey:              key.AppKey,
		Channel:             key.Channel,
		SerializationFormat: enums.SerializationJSON,
		FromAggregator:      "agg1",
		CommunicationId:     nil, // auto: newest only
	}
	req := buildJSONReq(t, "/receive-data-from-aggregator", body)
	w := httptest.NewRecorder()
	app.handleReceiveDataFromAggregator(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp ReceiveDataResponseBodyJSON
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Meta.CommunicationId != models.AutoCommIDPrefix+"10" {
		t.Errorf("expected commID=%s10, got %q", models.AutoCommIDPrefix, resp.Meta.CommunicationId)
	}
	// All 3 auto messages for agg1 must have been consumed (PullNewestAuto discards all).
	if handle.Incoming.Count() != 0 {
		t.Errorf("expected store empty after PullNewestAuto, got %d remaining", handle.Incoming.Count())
	}
}

// TestReceiveFromAggregator_ManualCommID_ReturnsSpecificMessage verifies that an explicit
// comm ID retrieves exactly the matching message while leaving unrelated messages in the store.
func TestReceiveFromAggregator_ManualCommID_ReturnsSpecificMessage(t *testing.T) {
	app, handle, key := newV2TestApp(t, false) // client

	clientID := shared.ClientID{}
	clientID[0] = 1

	// Target message.
	handle.Incoming.Store(bridge.IncomingMessage{
		CommunicationId:          "round-5",
		FromClientId:             clientID,
		FromAggregatorName:       "agg1",
		SrcSerializationModeByte: enums.SerializationJSONByte,
		SrcCompressionModeByte:   enums.CompressionNoneByte,
		TimestampArrived:         time.Now(),
		Data:                     []byte(`{"v":5}`),
	})
	// Unrelated: auto comm ID from same aggregator.
	storeIncomingAutoMsg(handle, "3", "agg1", clientID)
	// Unrelated: different manual comm ID from same aggregator.
	handle.Incoming.Store(bridge.IncomingMessage{
		CommunicationId:          "round-99",
		FromClientId:             clientID,
		FromAggregatorName:       "agg1",
		SrcSerializationModeByte: enums.SerializationJSONByte,
		SrcCompressionModeByte:   enums.CompressionNoneByte,
		TimestampArrived:         time.Now(),
		Data:                     []byte(`{"v":99}`),
	})

	commID := "round-5"
	body := ReceiveDataFromAggregatorRequestBody{
		AppKey:              key.AppKey,
		Channel:             key.Channel,
		SerializationFormat: enums.SerializationJSON,
		FromAggregator:      "agg1",
		CommunicationId:     &commID,
	}
	req := buildJSONReq(t, "/receive-data-from-aggregator", body)
	w := httptest.NewRecorder()
	app.handleReceiveDataFromAggregator(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp ReceiveDataResponseBodyJSON
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Meta.CommunicationId != "round-5" {
		t.Errorf("expected commID=round-5, got %q", resp.Meta.CommunicationId)
	}
	// The two unrelated messages must remain in the store.
	if remaining := handle.Incoming.Count(); remaining != 2 {
		t.Errorf("expected 2 unrelated messages still in store, got %d", remaining)
	}
}

// TestReceiveFromAggregator_AutoRetrievesAutoOnly_P2PRemains verifies that pulling with an
// automatic comm ID only consumes the auto message from the aggregator, leaving any manual
// peer-to-peer messages untouched — and that the P2P message can be retrieved afterwards.
func TestReceiveFromAggregator_AutoRetrievesAutoOnly_P2PRemains(t *testing.T) {
	app, handle, key := newV2TestApp(t, false) // client

	clientID := shared.ClientID{}
	clientID[0] = 1
	clientID2 := shared.ClientID{}
	clientID2[0] = 3

	// Auto message from the aggregator.
	storeIncomingAutoMsg(handle, "5", "agg1", clientID)

	// Manual P2P message from clientID2 — no aggregator involved.
	handle.Incoming.Store(bridge.IncomingMessage{
		CommunicationId:          "p2p-round-1",
		FromClientId:             clientID2,
		SrcSerializationModeByte: enums.SerializationJSONByte,
		SrcCompressionModeByte:   enums.CompressionNoneByte,
		TimestampArrived:         time.Now(),
		Data:                     []byte(`{"p2p":true}`),
	})

	// Receive with no comm ID — should return only the auto message.
	body := ReceiveDataFromAggregatorRequestBody{
		AppKey:              key.AppKey,
		Channel:             key.Channel,
		SerializationFormat: enums.SerializationJSON,
		FromAggregator:      "agg1",
		CommunicationId:     nil,
	}
	req := buildJSONReq(t, "/receive-data-from-aggregator", body)
	w := httptest.NewRecorder()
	app.handleReceiveDataFromAggregator(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("auto receive: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp ReceiveDataResponseBodyJSON
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("auto receive: decode response: %v", err)
	}
	if resp.Meta.CommunicationId != models.AutoCommIDPrefix+"5" {
		t.Errorf("auto receive: expected auto commID, got %q", resp.Meta.CommunicationId)
	}

	// The P2P message must still be in the store.
	if remaining := handle.Incoming.Count(); remaining != 1 {
		t.Fatalf("expected 1 P2P message still in store after auto pull, got %d", remaining)
	}

	// Retrieve the P2P message via the clients endpoint.
	p2pCommID := "p2p-round-1"
	minPkg := 1
	clientsBody := ReceiveDataFromClientsRequestBody{
		AppKey:              key.AppKey,
		Channel:             key.Channel,
		SerializationFormat: enums.SerializationJSON,
		CommunicationId:     &p2pCommID,
		MinPackages:         &minPkg,
	}
	req2 := buildJSONReq(t, "/receive-data-from-clients", clientsBody)
	w2 := httptest.NewRecorder()
	app.handleReceiveDataFromClients(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("P2P receive: expected 200, got %d: %s", w2.Code, w2.Body.String())
	}
	var result map[string][]ReceiveDataResponseBodyJSON
	if err := json.NewDecoder(w2.Body).Decode(&result); err != nil {
		t.Fatalf("P2P receive: decode response: %v", err)
	}
	if len(result["p2p-round-1"]) != 1 {
		t.Errorf("P2P receive: expected 1 message under p2p-round-1, got %d", len(result["p2p-round-1"]))
	}
	if handle.Incoming.Count() != 0 {
		t.Errorf("expected store empty after P2P pull, got %d remaining", handle.Incoming.Count())
	}
}

// TestSendToClients_TwoClientsP2PToOneClient_BothEnqueued verifies that two separate clients
// can each send a peer-to-peer message to the same target client using the same specific comm ID,
// with both messages correctly enqueued in their respective outgoing queues.
func TestSendToClients_TwoClientsP2PToOneClient_BothEnqueued(t *testing.T) {
	app1, handle1, key1 := newV2TestApp(t, false)
	app2, handle2, key2 := newV2TestApp(t, false)

	// clientID2 is the target recipient (matches fixture: clientID2[0]=3).
	targetID := shared.ClientID{}
	targetID[0] = 3
	commID := "p2p-42"

	send := func(app *AppCommunicatorV2, key bridge.RunKey) int {
		metadata := SendDataToClientsMetadata{
			AppKey:            key.AppKey,
			Channel:           key.Channel,
			SerializationUsed: enums.SerializationJSON,
			To:                []shared.ClientID{targetID},
			CommunicationId:   commID,
		}
		req := buildMultipartReq(t, "/send-data-to-clients", metadata, []byte(`"hello"`))
		w := httptest.NewRecorder()
		app.handleSendDataToClients(w, req)
		return w.Code
	}

	if code := send(app1, key1); code != http.StatusOK {
		t.Fatalf("client1 send: expected 200, got %d", code)
	}
	if code := send(app2, key2); code != http.StatusOK {
		t.Fatalf("client2 send: expected 200, got %d", code)
	}

	checkOutgoing := func(handle *bridge.FLRunHandle, label string) {
		msg, ok := handle.Outgoing.Dequeue()
		if !ok {
			t.Fatalf("%s: expected message in outgoing queue", label)
		}
		if *msg.DestinationClientID != targetID {
			t.Errorf("%s: expected dest=%v, got %v", label, targetID, *msg.DestinationClientID)
		}
		if string(msg.Memo[:msg.MemoSize]) != commID {
			t.Errorf("%s: expected commID=%q, got %q", label, commID, string(msg.Memo[:msg.MemoSize]))
		}
	}
	checkOutgoing(handle1, "client1")
	checkOutgoing(handle2, "client2")
}

// TestSendToClients_ToAggregator_Returns400 verifies that a client cannot send a peer-to-peer
// message directed at the aggregator (coordinator) — the endpoint only supports client targets.
func TestSendToClients_ToAggregator_Returns400(t *testing.T) {
	app, handle, key := newV2TestApp(t, false) // client

	aggID := handle.Meta.CoordinatorID
	commID := "p2p-to-agg"
	metadata := SendDataToClientsMetadata{
		AppKey:            key.AppKey,
		Channel:           key.Channel,
		SerializationUsed: enums.SerializationJSON,
		To:                []shared.ClientID{aggID},
		CommunicationId:   commID,
	}
	req := buildMultipartReq(t, "/send-data-to-clients", metadata, []byte(`"payload"`))
	w := httptest.NewRecorder()
	app.handleSendDataToClients(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestReceiveFromAggregator_NoMessages_Returns204 verifies that 204 No Content is returned
// when no matching message exists.
func TestReceiveFromAggregator_NoMessages_Returns204(t *testing.T) {
	app, _, key := newV2TestApp(t, false) // client

	body := ReceiveDataFromAggregatorRequestBody{
		AppKey:              key.AppKey,
		Channel:             key.Channel,
		SerializationFormat: enums.SerializationJSON,
		FromAggregator:      "agg1",
		CommunicationId:     nil, // auto: nothing stored
	}
	req := buildJSONReq(t, "/receive-data-from-aggregator", body)
	w := httptest.NewRecorder()
	app.handleReceiveDataFromAggregator(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d: %s", w.Code, w.Body.String())
	}
}

// ── handleReceiveDataFromClients ─────────────────────────────────────────────

// TestReceiveFromClients_MinPackagesNotMet_ReturnsEmptyMap verifies that when a comm ID group
// has fewer messages than minPackages, an empty map is returned and the messages are not consumed.
func TestReceiveFromClients_MinPackagesNotMet_ReturnsEmptyMap(t *testing.T) {
	app, handle, key := newV2TestApp(t, true) // coordinator

	clientID1 := shared.ClientID{}
	clientID1[0] = 10
	clientID2 := shared.ClientID{}
	clientID2[0] = 11

	storeIncomingAutoMsg(handle, "1", "", clientID1)
	storeIncomingAutoMsg(handle, "1", "", clientID2)

	minPkg := 3
	body := ReceiveDataFromClientsRequestBody{
		AppKey:              key.AppKey,
		Channel:             key.Channel,
		SerializationFormat: enums.SerializationJSON,
		MinPackages:         &minPkg,
	}
	req := buildJSONReq(t, "/receive-data-from-clients", body)
	w := httptest.NewRecorder()
	app.handleReceiveDataFromClients(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result map[string][]ReceiveDataResponseBodyJSON
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map (minPackages=3 but only 2 messages), got %d groups", len(result))
	}
	// Messages must remain in the store since the group did not qualify.
	if handle.Incoming.Count() != 2 {
		t.Errorf("expected 2 messages still in store, got %d", handle.Incoming.Count())
	}
}

// TestReceiveFromClients_MinPackagesMet_ReturnsGroup verifies that when a group has at least
// minPackages messages, the group is returned and the messages are consumed from the store.
func TestReceiveFromClients_MinPackagesMet_ReturnsGroup(t *testing.T) {
	app, handle, key := newV2TestApp(t, true) // coordinator

	clientID1 := shared.ClientID{}
	clientID1[0] = 10
	clientID2 := shared.ClientID{}
	clientID2[0] = 11

	storeIncomingAutoMsg(handle, "1", "", clientID1)
	storeIncomingAutoMsg(handle, "1", "", clientID2)

	minPkg := 2
	autoStr := models.RequestAutoCommId
	body := ReceiveDataFromClientsRequestBody{
		AppKey:              key.AppKey,
		Channel:             key.Channel,
		SerializationFormat: enums.SerializationJSON,
		CommunicationId:     &autoStr,
		MinPackages:         &minPkg,
	}
	req := buildJSONReq(t, "/receive-data-from-clients", body)
	w := httptest.NewRecorder()
	app.handleReceiveDataFromClients(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result map[string][]ReceiveDataResponseBodyJSON
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result))
	}
	msgs := result[models.AutoCommIDPrefix+"1"]
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages in group, got %d", len(msgs))
	}
	if handle.Incoming.Count() != 0 {
		t.Errorf("expected store empty after qualifying pull, got %d remaining", handle.Incoming.Count())
	}
}
