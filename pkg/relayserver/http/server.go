// Package http implements the HTTP API server for the global relay server.
// It exposes endpoints to create and stop FL runs, delegating all state to the
// shared FlRunStore. Stopping a run signals the TCP relay goroutines to exit cleanly.
package http

import (
	"encoding/json"
	"errors"
	"fmt"
	nethttp "net/http"

	bridge "fc_controller/pkg/relayserver/bridge"
	enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	"fc_controller/pkg/shared/util"

	"github.com/gorilla/mux"
)

const GLOBAL = "GLOBAL"

// RelayServiceHTTP is the HTTP API server for the relay.
// It shares a FlRunStore with the TCP server so both see the same FL runs.
type RelayServiceHTTP struct {
	store    *bridge.FlRunStore
	listener *nethttp.Server
}

func NewHTTPServer(store *bridge.FlRunStore) *RelayServiceHTTP {
	return &RelayServiceHTTP{store: store}
}

// StartServer starts the HTTP API and blocks until it exits.
func (s *RelayServiceHTTP) StartServer(port int) error {
	r := mux.NewRouter()
	r.HandleFunc("/healthz", s.handleHealthz).Methods(nethttp.MethodGet, nethttp.MethodHead)
	r.HandleFunc("/create-fl-run", s.handleCreateFLRun).Methods(nethttp.MethodPost)
	r.HandleFunc("/sign-fl-run-cert", s.handleSignFLRunCert).Methods(nethttp.MethodPost)
	r.HandleFunc("/stop-fl-run", s.handleStopFLRun).Methods(nethttp.MethodPost)
	logger.Info(GLOBAL, "", "HTTP API listening on port %d", port)

	server := nethttp.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", port),
		Handler: r,
	}
	s.listener = &server
	return server.ListenAndServe()
}

func (s *RelayServiceHTTP) Shutdown() error {
	if s.listener == nil {
		return nil
	}
	return s.listener.Close()
}

// ---------------------------------------------------------------------------
// Meta Endpoints (e.g. healthz)
// ---------------------------------------------------------------------------

func (s *RelayServiceHTTP) handleHealthz(w nethttp.ResponseWriter, r *nethttp.Request) {
	util.HandleHealthz(w)
}

// ---------------------------------------------------------------------------
// Create FL Run
// ---------------------------------------------------------------------------

type createFLRunRequest struct {
	MaxNumClients int    `json:"maxNumClients"`
	AppVersion    string `json:"appVersion"`
}

type createFLRunResponse struct {
	CoordinatorID      shared.ClientID        `json:"coordinatorId"`
	CoordinatorKey     string                 `json:"coordinatorKey"`
	ClientIDs          []shared.ClientID      `json:"clientIds"`
	ClientId2ClientKey map[string]util.APIKey `json:"clientId2ClientKey"`
	Channel            string                 `json:"channel"`
	// RelayKey is the relay server's own API key for this run. Clients use it to
	// verify the identity of the relay server during the TCP connection handshake.
	RelayKey string `json:"relayKey"`
}

// handleCreateFLRun creates a new FL run
// The FL run is persisted in the in memory FlRunStore
// and returns the coordinator ID, client IDs, and their corresponding API keys.
func (s *RelayServiceHTTP) handleCreateFLRun(w nethttp.ResponseWriter, r *nethttp.Request) {
	defer r.Body.Close()
	logger.Info(GLOBAL, "", "Received request to create FL run")
	var req createFLRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		nethttp.Error(w, "invalid JSON body", nethttp.StatusBadRequest)
		return
	}

	if req.MaxNumClients < 1 {
		nethttp.Error(w, "invalid field maxNumClients, expected int >= 1", nethttp.StatusBadRequest)
		return
	}

	appVersion, err := enums.StringToAppVersionEnum(req.AppVersion)
	if err != nil {
		nethttp.Error(w, "invalid field appVersion, expected one of: v1, v2", nethttp.StatusBadRequest)
		return
	}

	flRun, err := s.store.Create(req.MaxNumClients, appVersion)
	if err != nil {
		nethttp.Error(w, "failed to create FL run: "+err.Error(), nethttp.StatusInternalServerError)
		return
	}

	clientId2ClientKey := make(map[string]util.APIKey, len(flRun.ClientId2ClientKey))
	for clientID, clientKey := range flRun.ClientId2ClientKey {
		clientId2ClientKey[clientID.ToString()] = clientKey
	}

	resp := createFLRunResponse{
		CoordinatorID:      flRun.CoordinatorID,
		CoordinatorKey:     flRun.CoordinatorKey.String(),
		ClientIDs:          flRun.ClientIDOrder,
		ClientId2ClientKey: clientId2ClientKey,
		Channel:            flRun.Channel.ToString(),
		RelayKey:           flRun.FlRunBase.RelayKey.String(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(nethttp.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Error(GLOBAL, "", "failed to encode create-fl-run response: %v", err)
		return
	}

	logger.Info(GLOBAL, "", "FL run created successfully: %+v", resp)
}

// ---------------------------------------------------------------------------
// Sign FL Run client certificate
// ---------------------------------------------------------------------------

// maxSignRequestBytes limits the body of a sign request, a CSR is well below 1 KiB.
const maxSignRequestBytes = 16 * 1024

type signFLRunCertRequest struct {
	Channel  string          `json:"channel"`
	ClientID shared.ClientID `json:"clientId"`
	// CSR is the PEM encoded certificate signing request of the client's controller.
	CSR string `json:"csr"`
}

type signFLRunCertResponse struct {
	// Certificate is the PEM encoded client certificate, signed by the CA of the FL run.
	Certificate string `json:"certificate"`
}

// handleSignFLRunCert signs the CSR of a client with the CA of the FL run identified by
// the channel. The client's controller needs the certificate to connect to the TCP relay.
func (s *RelayServiceHTTP) handleSignFLRunCert(w nethttp.ResponseWriter, r *nethttp.Request) {
	defer r.Body.Close()

	var req signFLRunCertRequest
	if err := json.NewDecoder(nethttp.MaxBytesReader(w, r.Body, maxSignRequestBytes)).Decode(&req); err != nil {
		nethttp.Error(w, "invalid JSON body", nethttp.StatusBadRequest)
		return
	}

	channel, err := shared.ChannelFromString(req.Channel)
	if err != nil {
		nethttp.Error(w, "invalid field channel", nethttp.StatusBadRequest)
		return
	}

	flRun, exists := s.store.Get(channel)
	if !exists {
		nethttp.Error(w, "no FL run found for channel", nethttp.StatusNotFound)
		return
	}

	certPEM, err := flRun.SignClientCert(req.ClientID, []byte(req.CSR))
	if err != nil {
		logger.Warn(GLOBAL, "", "Refused to sign certificate for client %s: %v", req.ClientID.ToString(), err)
		switch {
		case errors.Is(err, util.ErrInvalidCSR):
			nethttp.Error(w, err.Error(), nethttp.StatusBadRequest)
		case errors.Is(err, bridge.ErrUnknownClient), errors.Is(err, shared.ErrStopped):
			nethttp.Error(w, "client does not belong to an active FL run", nethttp.StatusNotFound)
		case errors.Is(err, bridge.ErrKeyAlreadyBound):
			nethttp.Error(w, err.Error(), nethttp.StatusConflict)
		default:
			nethttp.Error(w, "failed to sign certificate", nethttp.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(nethttp.StatusOK)
	if err := json.NewEncoder(w).Encode(signFLRunCertResponse{Certificate: string(certPEM)}); err != nil {
		logger.Error(GLOBAL, "", "failed to encode sign-fl-run-cert response: %v", err)
		return
	}

	logger.Info(GLOBAL, "", "Signed certificate for client %s", req.ClientID.ToString())
}

// ---------------------------------------------------------------------------
// Stop FL Run
// ---------------------------------------------------------------------------

type stopFLRunRequest struct {
	Channel string `json:"channel"`
}

// handleStopFLRun removes the run from the store and signals all relay goroutines
// for that run to exit cleanly via FlRunMeta.Stop() → state set to finished → goroutines exit.
func (s *RelayServiceHTTP) handleStopFLRun(w nethttp.ResponseWriter, r *nethttp.Request) {
	defer r.Body.Close()

	var req stopFLRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		nethttp.Error(w, "invalid JSON body", nethttp.StatusBadRequest)
		return
	}

	channel, err := shared.ChannelFromString(req.Channel)
	if err != nil {
		nethttp.Error(w, "invalid field channel", nethttp.StatusBadRequest)
		return
	}

	// Stop() removes from the store, marks state as finished, and closes all connections.
	// Relay goroutines see IsActive() = false and exit cleanly.
	s.store.Stop(channel)

	w.WriteHeader(nethttp.StatusOK)
}
