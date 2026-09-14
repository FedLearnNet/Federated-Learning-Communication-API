package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/models"
	shared_enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	util "fc_controller/pkg/shared/util"
)

// --- HTTP test infrastructure ---

// newTestServer creates an FLRunManagerServiceHTTP and wraps its handler in an httptest.Server.
// Uses port 0 for both servers so the OS picks a free port; AppCommV2 is never started.
func newTestServer(t *testing.T) (*FLRunManagerServiceHTTP, *httptest.Server) {
	t.Helper()
	s := NewFLRunManagerServiceHTTP(0, 0, "", "127.0.0.1:19999", "prod", "on")
	ts := httptest.NewServer(s.server.Handler)
	t.Cleanup(ts.Close)
	return s, ts
}

func postJSON(t *testing.T, ts *httptest.Server, path string, body []byte) *http.Response {
	t.Helper()
	resp, err := http.Post(ts.URL+path, "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func expectStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Errorf("expected status %d, got %d", want, resp.StatusCode)
	}
}

// --- fixtures ---

func newChannel(t *testing.T) shared.RelayChannel {
	t.Helper()
	return shared.NewChannel()
}

func newClientID(t *testing.T) shared.ClientID {
	t.Helper()
	id, err := shared.NewClientID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newAPIKey(t *testing.T) util.APIKey {
	t.Helper()
	k, err := util.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// --- HTTP server tests ---

func TestNewFLRunManagerServiceHTTP(t *testing.T) {
	s := NewFLRunManagerServiceHTTP(8080, 8081, "", "127.0.0.1:19999", "prod", "on")
	if s == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestHTTPServer_Index(t *testing.T) {
	_, ts := newTestServer(t)
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	expectStatus(t, resp, http.StatusOK)
}

func TestHTTPServer_Healthz(t *testing.T) {
	_, ts := newTestServer(t)
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	expectStatus(t, resp, http.StatusOK)
	var body util.Health
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode healthz response: %v", err)
	}
	if body.Status != "UP" {
		t.Errorf("expected status UP, got %s", body.Status)
	}
}

func TestHTTPServer_StartLearning_InvalidJSON(t *testing.T) {
	_, ts := newTestServer(t)
	resp := postJSON(t, ts, "/start-learning", []byte("not json"))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestHTTPServer_StartLearning_MissingChannel(t *testing.T) {
	_, ts := newTestServer(t)
	body, _ := json.Marshal(map[string]interface{}{
		"relayKey": "somekey",
	})
	resp := postJSON(t, ts, "/start-learning", body)
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestHTTPServer_StartLearning_MissingRelayKey(t *testing.T) {
	_, ts := newTestServer(t)
	clientId := newClientID(t)
	body, _ := json.Marshal(models.FLExperiment{
		Channel:        newChannel(t).ToString(),
		ClientId:       clientId,
		ClientKey:      newAPIKey(t),
		RunId:          "run-1",
		CoordinatorId:  clientId,
		MaxNumClients:  1,
		OrderClientIds: []shared.ClientID{clientId},
		AppKey:         newAPIKey(t),
		AppVersion:     shared_enums.AppVersionV2,
		// RelayKey intentionally omitted — should return 400
	})
	resp := postJSON(t, ts, "/start-learning", body)
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestHTTPServer_StopLearning_InvalidJSON(t *testing.T) {
	_, ts := newTestServer(t)
	resp := postJSON(t, ts, "/stop-learning", []byte("not json"))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestHTTPServer_StopLearning_MissingChannel(t *testing.T) {
	_, ts := newTestServer(t)
	body, _ := json.Marshal(StopLearningRequest{
		AppKey: "somekey",
	})
	resp := postJSON(t, ts, "/stop-learning", body)
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestHTTPServer_StopLearning_MissingAppKey(t *testing.T) {
	_, ts := newTestServer(t)
	body, _ := json.Marshal(StopLearningRequest{
		Channel: newChannel(t).ToString(),
	})
	resp := postJSON(t, ts, "/stop-learning", body)
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestHTTPServer_StopLearning_NotFound(t *testing.T) {
	_, ts := newTestServer(t)
	body, _ := json.Marshal(StopLearningRequest{
		Channel: newChannel(t).ToString(),
		AppKey:  "some-app-key",
	})
	resp := postJSON(t, ts, "/stop-learning", body)
	expectStatus(t, resp, http.StatusNotFound)
}

// --- BO unit tests ---

func TestFLRunManagerBo_StopRun_NotFound(t *testing.T) {
	bo := NewFLRunManagerBO("", "127.0.0.1:19999", 0, "prod", "on")
	_, _, err := bo.StopRun(bridge.RunKey{Channel: newChannel(t), AppKey: "nonexistent"})
	var notFoundErr models.NotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected NotFoundError, got %T: %v", err, err)
	}
}

// TestFLRunManagerBo_StartRun_Conflict pre-inserts an entry directly (same-package access)
// and verifies that StartRun returns ConflictError for the same run key without touching crypto.
func TestFLRunManagerBo_StartRun_Conflict(t *testing.T) {
	bo := NewFLRunManagerBO("", "127.0.0.1:19999", 0, "prod", "on")
	ch := newChannel(t)
	key := bridge.RunKey{Channel: ch, AppKey: "myapp"}
	bo.flRuns[key] = flRunOrch{} // simulate a run already in progress

	err := bo.StartRun(&models.NormalizedFLExperiment{
		Channel:        ch,
		AppKey:         "myapp",
		ClientId:       newClientID(t),
		ClientKey:      "clientkey",
		RelayKey:       "relaykey",
		CoordinatorId:  newClientID(t),
		MaxNumClients:  1,
		OrderClientIds: []shared.ClientID{newClientID(t)},
		AppVersion:     shared_enums.AppVersionV2,
		RunId:          "run-1",
	})

	var conflictErr models.ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected ConflictError, got %T: %v", err, err)
	}
}

// TestFLRunManagerBo_StopRun_Success inserts a real FLRunHandle, calls StopRun, and
// verifies the handle is marked finished, discarded counts are returned, and the
// run is removed from the BO map.
func TestFLRunManagerBo_StopRun_Success(t *testing.T) {
	bo := NewFLRunManagerBO("", "127.0.0.1:19999", 0, "prod", "on")
	ch := newChannel(t)
	key := bridge.RunKey{Channel: ch, AppKey: "myapp"}

	clientId := newClientID(t)
	coordId := newClientID(t)
	privKey, pubKey, err := util.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	meta, err := bridge.NewFlRunMeta(
		ch, "clientkey", "relaykey",
		coordId, clientId, false,
		1, []shared.ClientID{clientId},
		pubKey, privKey, shared_enums.AppVersionV2,
	)
	if err != nil {
		t.Fatal(err)
	}

	handle := bridge.NewFLRunHandle(meta)
	bo.flRuns[key] = flRunOrch{flRunHandle: handle}

	incoming, outgoing, err := bo.StopRun(key)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if incoming != 0 || outgoing != 0 {
		t.Errorf("expected 0 discarded messages, got incoming=%d outgoing=%d", incoming, outgoing)
	}
	if handle.IsActive() {
		t.Error("expected handle to be inactive after StopRun")
	}
	if _, exists := bo.flRuns[key]; exists {
		t.Error("expected run to be removed from map after StopRun")
	}
}
