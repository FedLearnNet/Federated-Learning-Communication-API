// A websocket client that connects to the learning api to simulate the communication
// of the app v2 for v1 apps
package learningcomm

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/shared/logger"
)

const LEARNING_API string = "LEARNING_API"

// Websocket timing constants
const (
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	writeWait  = 10 * time.Second
)

// UpdateCB is invoked when the app reports a progress, state, or message change.
type UpdateCB func(message *string, progress *float64, state *string, msgType AppMessageTypeEnum)

type AppMessageTypeEnum string

const (
	StartRun   AppMessageTypeEnum = "START_RUN"
	UpdateRun  AppMessageTypeEnum = "UPDATE_RUN"
	LogMessage AppMessageTypeEnum = "LOG_MESSAGE"
	FinishRun  AppMessageTypeEnum = "FINISH_RUN"
)

type AppRunTypeEnum string

const (
	ExperimentRun AppRunTypeEnum = "EXPERIMENT_RUN"
	ProjectRun    AppRunTypeEnum = "PROJECT_RUN"
	FederatedRun  AppRunTypeEnum = "FEDERATED_RUN"
	NotDefined    AppRunTypeEnum = "NOT_DEFINED"
)

type AppMessageWrapper struct {
	Type    AppMessageTypeEnum `json:"type"`
	Message interface{}        `json:"message"`
	RunType AppRunTypeEnum     `json:"runType"`
}

type LearningAPIClient struct {
	wsConn                 *websocket.Conn
	wsMutex                sync.Mutex
	LearningApiWsSuffixURL string `json:"learningApiWsSuffixURL"`
	LearningApiWsToken     string `json:"learningApiWsToken"`
	handle                 *bridge.FLRunHandle
}

// NewLearningAPIClient initializes a new LearningAPIClient
func NewLearningAPIClient(learningApiWsSuffixURL string, learningApiWsToken string) *LearningAPIClient {
	return &LearningAPIClient{
		LearningApiWsSuffixURL: strings.Trim(learningApiWsSuffixURL, "\""),
		LearningApiWsToken:     strings.Trim(learningApiWsToken, "\""),
	}
}

// StartClient establishes a WebSocket connection to the learning API
// It receives an FLRunHandle which contains the state object shared with other components.
// The connection is automatically stopped when the connected flRun is marked as done (inactive).
func (l *LearningAPIClient) StartClient(handle *bridge.FLRunHandle, learningApiBaseURL string, runId string) error {
	if handle == nil {
		return fmt.Errorf("FLRunHandle cannot be nil")
	}
	l.handle = handle

	learningApiUrl := learningApiBaseURL + l.LearningApiWsSuffixURL
	logger.Info(LEARNING_API, runId, "Connecting to LocalLearningAPI at %s", learningApiUrl)
	var header map[string][]string
	if l.LearningApiWsToken != "" {
		logger.Debug(LEARNING_API, runId, "Using LearningAPI token for WebSocket connection")
		header = map[string][]string{
			"Authorization": {"Bearer " + l.LearningApiWsToken},
		}
	}
	conn, resp, err := websocket.DefaultDialer.Dial(learningApiUrl, header)
	if err != nil {
		var errMsg string
		if resp != nil {
			errMsg = fmt.Sprintf("HTTP Response Status: %d %s. Error: %s", resp.StatusCode, resp.Status, err)
		} else {
			errMsg = fmt.Sprintf("Connection error: %s", err)
		}
		logger.Error(LEARNING_API, runId, "Failed to establish WebSocket connection with LocalLearningAPI at %s: %s", learningApiBaseURL, errMsg)
		return fmt.Errorf("failed to connect to learning API: %w", err)
	}

	conn.SetReadLimit(4 << 20) // 4 MB safety
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(appData string) error {
		logger.Debug(LEARNING_API, runId, "PONG from server: %q", appData)
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	conn.SetPingHandler(func(appData string) error {
		logger.Debug(LEARNING_API, runId, "PING from server: %q", appData)
		deadline := time.Now().Add(writeWait)
		return conn.WriteControl(websocket.PongMessage, []byte(appData), deadline)
	})
	conn.SetCloseHandler(func(code int, text string) error {
		logger.Warn(LEARNING_API, runId, "CLOSE frame received: code=%d text=%q", code, text)
		return nil
	})

	l.wsMutex.Lock()
	l.wsConn = conn
	l.wsMutex.Unlock()
	go l.startWSReader(runId, conn)

	return nil
}

/*
	LEARNING API (app v1 only) --> CONTROLLER
*/

// startWSReader continuously reads messages from the learning API websocket connection and
// logs them. It monitors the FLRunHandle state and stops reading when the run becomes inactive.
// The messages do not invoke any changes, they are just logged.
// To start/stop a run the HTTP endpoints must be used.
func (m *LearningAPIClient) startWSReader(runId string, c *websocket.Conn) {
	defer func() {
		_ = c.Close()
		logger.Info(LEARNING_API, runId, "WS reader stopped, connection closed")
	}()

	for {
		// Check if the run is still active before attempting to read
		if m.handle != nil && !m.handle.IsActive() {
			logger.Info(LEARNING_API, runId, "Run is no longer active, stopping WS reader")
			return
		}

		// Set a read deadline so we can periodically check if the run is still active
		_ = c.SetReadDeadline(time.Now().Add(pongWait))

		mt, message, err := c.ReadMessage()
		if err != nil {
			// classify
			var ne net.Error
			switch {
			case errors.Is(err, io.EOF):
				logger.Warn(LEARNING_API, runId, "Read EOF: peer closed connection")
			case errors.As(err, &ne) && ne.Timeout():
				// Timeout is expected due to our SetReadDeadline; this allows us to check run state
				continue // Retry on timeout
			case websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseNoStatusReceived,
				websocket.CloseAbnormalClosure,
				websocket.ClosePolicyViolation,
				websocket.CloseMessageTooBig,
				websocket.CloseInternalServerErr):
				logger.Warn(LEARNING_API, runId, "CloseError: %v", err)
			case websocket.IsUnexpectedCloseError(err):
				logger.Error(LEARNING_API, runId, "Unexpected close error: %v", err)
			default:
				logger.Error(LEARNING_API, runId, "Read error: %T %v", err, err)
			}
			return
		}
		logger.Debug(LEARNING_API, runId, "WS recv type=%d len=%d preview=%q",
			mt, len(message), preview(message, 256))
	}
}

// preview is just used for debug logging
func preview(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "...(truncated)"
}

// closeConnection closes the WebSocket connection for the run.
// The FLRunHandle state will signal the reader to stop on its next check.
func (l *LearningAPIClient) closeConnection(runId string) {
	l.wsMutex.Lock()
	defer l.wsMutex.Unlock()

	if l.wsConn != nil {
		logger.Info(LEARNING_API, runId, "Closing WebSocket connection")
		_ = l.wsConn.Close()
		l.wsConn = nil
	}
}

// Stop cleanly shuts down the learning API client and closes the connection.
// Should be called when the FL run is complete or needs to be terminated.
func (l *LearningAPIClient) Stop(runId string) {
	logger.Info(LEARNING_API, runId, "Stopping LearningAPIClient")
	l.closeConnection(runId)
}
