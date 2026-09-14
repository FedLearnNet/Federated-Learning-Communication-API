// Orchestration provides HTTP endpoints for the orchestration of federated learning runs
package controller

import (
	"context"
	"encoding/json"
	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/models"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	util "fc_controller/pkg/shared/util"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

const READTIMEOUT = 60 * time.Second
const WRITETIMEOUT = 180 * time.Second
const FLRUNMANAGERSERVICE = "FL_RUN_MANAGER_SERVICE" // for logging

type FLRunManagerServiceHTTP struct {
	port          int
	server        *http.Server
	flManager     *FLRunManagerBo
	queryInterval time.Duration
}

func NewFLRunManagerServiceHTTP(flrunmanagerport int, appcommv2port int, learningAPIWSBaseUrl string, globalAddress string, mode string, tlsMode string) *FLRunManagerServiceHTTP {
	s := &FLRunManagerServiceHTTP{
		port: flrunmanagerport,
	}

	// SETUP: the flrun manager HTTP server
	r := mux.NewRouter()

	// Simple methods, no error handling so no handleRequest wrapper needed
	r.HandleFunc("/", s.handleIndex).Methods(http.MethodGet) // Simple debug hello world endpoint
	r.HandleFunc("/healthz", s.handleHealthz).Methods(http.MethodGet, http.MethodHead)

	// More complex
	r.HandleFunc("/start-learning", s.handleRequest(s.handleStartLearning)).Methods(http.MethodPost)
	r.HandleFunc("/stop-learning", s.handleRequest(s.handleStopLearning)).Methods(http.MethodPost)
	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", flrunmanagerport),
		Handler:      r,
		ReadTimeout:  READTIMEOUT,
		WriteTimeout: WRITETIMEOUT,
	}

	// SETUP: the FLRunManagerBo
	bo := NewFLRunManagerBO(learningAPIWSBaseUrl, globalAddress, appcommv2port, mode, tlsMode)
	s.flManager = bo
	return s
}

func (s *FLRunManagerServiceHTTP) StartServer() error {
	// Start AppCommunicatorV2 first (non-blocking: it launches its own goroutine)
	if err := s.flManager.StartAppCommV2(); err != nil {
		logger.Error(FLRUNMANAGERSERVICE, "", "Failed to start AppCommunicatorV2: %v", err)
		return err
	}
	// Start orchestration HTTP server (blocks until shutdown)
	err := s.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		logger.Error(FLRUNMANAGERSERVICE, "", "FLRunManagerServiceHTTP stopped: %v", err)
		return err
	}
	return nil
}

func (s *FLRunManagerServiceHTTP) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// handleRequest wraps a handler to catch errors and translate them to HTTP status codes.
func (s *FLRunManagerServiceHTTP) handleRequest(f func(w http.ResponseWriter, r *http.Request) error) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		err := f(w, r)
		if err != nil {
			s.handleError(err, w)
		}
	}
}

func (s *FLRunManagerServiceHTTP) handleHealthz(w http.ResponseWriter, r *http.Request) {
	util.HandleHealthz(w)
}

func (s *FLRunManagerServiceHTTP) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello from FedDB-FeatureCloud Controller!"))
}

func (s *FLRunManagerServiceHTTP) handleStartLearning(w http.ResponseWriter, r *http.Request) error {
	body, _ := io.ReadAll(r.Body)

	logger.Debug(FLRUNMANAGERSERVICE, "", "/start-learning request received")
	var experiment models.FLExperiment
	err := json.Unmarshal(body, &experiment)
	if err != nil {
		logger.Warn(FLRUNMANAGERSERVICE, "", "Failed to unmarshal experiment data: %v. Body: %s", err, string(body))
		return models.ValidationError{Message: err.Error()}
	}
	logger.Debug(FLRUNMANAGERSERVICE, "", "Experiment data unmarshalled successfully: %+v", experiment)
	normalizedExperiment, err := experiment.Normalize()
	if err != nil {
		logger.Warn(FLRUNMANAGERSERVICE, "", "Failed to normalize experiment data: %v", err)
		return models.ValidationError{Message: err.Error()}
	}

	err = s.flManager.StartRun(normalizedExperiment)
	if err != nil {
		logger.Error(FLRUNMANAGERSERVICE, "", "Failed to start learning: %v", err)
		return err
	}

	w.WriteHeader(http.StatusOK)

	return nil
}

type StopLearningRequest struct {
	Channel string      `json:"channel"`
	AppKey  util.APIKey `json:"appKey"`
}

type StopLearningResponse struct {
	IncomingDiscarded int `json:"incomingDiscarded"`
	OutgoingDiscarded int `json:"outgoingDiscarded"`
}

func (s *FLRunManagerServiceHTTP) handleStopLearning(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return models.ValidationError{Message: "failed to read request body"}
	}

	var req StopLearningRequest
	if err := json.Unmarshal(body, &req); err != nil {
		logger.Warn(FLRUNMANAGERSERVICE, "", "Failed to unmarshal stop-learning body: %v. Body: %s", err, string(body))
		return models.ValidationError{Message: err.Error()}
	}
	if req.Channel == "" {
		return models.ValidationError{Message: "channel must not be empty"}
	}
	if req.AppKey == "" {
		return models.ValidationError{Message: "appKey must not be empty"}
	}

	channel, err := shared.ChannelFromString(req.Channel)
	if err != nil {
		return models.ValidationError{Message: fmt.Sprintf("invalid channel: %v", err)}
	}

	runKey := bridge.RunKey{
		Channel: channel,
		AppKey:  req.AppKey,
	}

	incoming, outgoing, err := s.flManager.StopRun(runKey)
	if err != nil {
		logger.Error(FLRUNMANAGERSERVICE, "", "Failed to stop learning: %v", err)
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(StopLearningResponse{
		IncomingDiscarded: incoming,
		OutgoingDiscarded: outgoing,
	})
	return nil
}

func (s *FLRunManagerServiceHTTP) handleError(err error, w http.ResponseWriter) {
	switch err.(type) {
	case models.ValidationError:
		w.WriteHeader(400)
	case models.ConflictError:
		w.WriteHeader(409)
	case models.NotFoundError:
		w.WriteHeader(404)
	default:
		w.WriteHeader(500)
	}

	type errorResponse struct {
		Detail string `json:"detail"`
	}
	errRes := errorResponse{
		Detail: err.Error(),
	}
	b, _ := json.Marshal(errRes)
	_, _ = w.Write(b)
}
