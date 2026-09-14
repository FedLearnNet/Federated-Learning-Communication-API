package controller

import (
	"fc_controller/pkg/controller/appcomm"
	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/models"
	"fc_controller/pkg/controller/relaycomm"
	shared_enums "fc_controller/pkg/shared/enums"
	"fc_controller/pkg/shared/logger"
	"fmt"
	"sync"
	"time"
)

const FLRUNMANAGERBO = "FL_RUN_MANAGER_BO"
const QUERY_INTERVAL_SECONDS = 1 * time.Second

type FLRunManagerBo struct {
	globalAddress        string
	learningApiWSBaseUrl string
	queryInterval        time.Duration
	flRuns               map[bridge.RunKey]flRunOrch
	appCommunicatorV2    *appcomm.AppCommunicatorV2
	appcommport          int
	mode                 string
	tlsMode              string
	flRunLock            sync.RWMutex
}

type flRunOrch struct {
	flRunHandle *bridge.FLRunHandle
	progress    float64
}

func NewFLRunManagerBO(learningApiWSBaseUrl string, globalAddress string, appCommPort int, mode string, tlsMode string) *FLRunManagerBo {
	return &FLRunManagerBo{
		learningApiWSBaseUrl: learningApiWSBaseUrl,
		globalAddress:        globalAddress,
		queryInterval:        QUERY_INTERVAL_SECONDS,
		flRuns:               make(map[bridge.RunKey]flRunOrch),
		appCommunicatorV2:    appcomm.NewAppCommunicatorV2(appCommPort),
		appcommport:          appCommPort,
		mode:                 mode,
		tlsMode:              tlsMode,
	}
}

func (flManager *FLRunManagerBo) StartAppCommV2() error {
	go func() {
		err := flManager.appCommunicatorV2.StartServer()
		if err != nil {
			logger.Fatal(FLRUNMANAGERBO, "", "Failed to start AppCommunicatorV2 server: %v", err)
		}
	}()
	return nil
}

// StartRun creates and starts all components for a new FL run.
// For V2: creates handle, relay client, registers with AppCommunicatorV2, and connects async.
func (flManager *FLRunManagerBo) StartRun(experiment *models.NormalizedFLExperiment) error {
	logger.Info(FLRUNMANAGERBO, experiment.RunId, "Starting run")

	runKey := bridge.RunKey{
		Channel: experiment.Channel,
		AppKey:  experiment.AppKey,
	}

	flManager.flRunLock.Lock()
	if _, exists := flManager.flRuns[runKey]; exists {
		flManager.flRunLock.Unlock()
		return models.ConflictError{
			Message: fmt.Sprintf("run with channel %s and app key %s already exists", experiment.Channel, experiment.AppKey),
		}
	}
	flManager.flRunLock.Unlock()

	switch experiment.AppVersion {
	case shared_enums.AppVersionV1:
		logger.Error(FLRUNMANAGERBO, experiment.RunId, "App version v1 is currently not supported")
		return fmt.Errorf("app version v1 is currently not supported")

	case shared_enums.AppVersionV2:
		meta, err := bridge.NewFlRunMeta(
			experiment.Channel,
			experiment.ClientKey,
			experiment.RelayKey,
			experiment.CoordinatorId,
			experiment.ClientId,
			experiment.ClientId == experiment.CoordinatorId,
			experiment.MaxNumClients,
			experiment.OrderClientIds,
			experiment.OwnPublicKey,
			experiment.OwnPrivateKey,
			experiment.AppVersion,
		)
		if err != nil {
			return fmt.Errorf("failed to create FL run meta: %w", err)
		}

		handle := bridge.NewFLRunHandle(meta)

		relayClient, err := relaycomm.NewClient(
			flManager.queryInterval,
			handle,
			"",
			experiment.Channel,
			experiment.MaxNumClients,
			flManager.globalAddress,
			flManager.mode,
			flManager.tlsMode,
		)
		if err != nil {
			return fmt.Errorf("failed to create relay client: %w", err)
		}

		flManager.flRunLock.Lock()
		flManager.flRuns[runKey] = flRunOrch{flRunHandle: handle, progress: 0}
		flManager.flRunLock.Unlock()

		flManager.appCommunicatorV2.AddRun(runKey, handle)

		if err := relayClient.StartClient(); err != nil {
			logger.Error(FLRUNMANAGERBO, experiment.RunId, "relay client failed to connect: %v", err)
			handle.MarkError()
			flManager.flRunLock.Lock()
			delete(flManager.flRuns, runKey)
			flManager.flRunLock.Unlock()
			flManager.appCommunicatorV2.RemoveRun(runKey)
			return fmt.Errorf("failed to start relay client: %w", err)
		}

		logger.Info(FLRUNMANAGERBO, experiment.RunId, "Run started, connecting to relay server")
		return nil
	}

	return fmt.Errorf("unknown app version: %v", experiment.AppVersion)
}

// StopRun marks the run as finished, which cascades to all components.
// Returns the counts of discarded incoming and outgoing messages.
// Returns NotFoundError if the run does not exist.
func (flManager *FLRunManagerBo) StopRun(runKey bridge.RunKey) (int, int, error) {
	flManager.flRunLock.Lock()
	orch, exists := flManager.flRuns[runKey]
	if !exists {
		flManager.flRunLock.Unlock()
		logger.Info(FLRUNMANAGERBO, "", "StopRun called for unknown run (already stopped or never started): channel=%s", runKey.Channel.ToString())
		return 0, 0, models.NotFoundError{
			Message: fmt.Sprintf("run with channel %s and app key %s not found", runKey.Channel.ToString(), runKey.AppKey),
		}
	}
	handle := orch.flRunHandle
	delete(flManager.flRuns, runKey)
	flManager.flRunLock.Unlock()

	// MarkFinished cascades: unblocks readIncomming and watchOutgoing goroutines,
	// all future Safe* calls return false/nil, AppCommunicatorV2 handlers return 409.
	_, incoming, outgoing := handle.MarkFinished()
	logger.Info(FLRUNMANAGERBO, "", "Run stopped (incoming discarded: %d, outgoing discarded: %d)", incoming, outgoing)

	flManager.appCommunicatorV2.RemoveRun(runKey)
	return incoming, outgoing, nil
}
