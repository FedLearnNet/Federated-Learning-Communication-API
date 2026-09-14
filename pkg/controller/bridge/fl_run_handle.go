package bridge

import (
	"fc_controller/pkg/controller/models"
	"fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	"fc_controller/pkg/shared/util"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const BRIDGE = "BRIDGE"

// RunKey uniquely identifies an FL run within the AppCommunicatorV2.
type RunKey struct {
	Channel shared.RelayChannel
	AppKey  util.APIKey
}

// FLRunHandle is the shared communication bridge between the RelayClient and the app communicators
// for one FL run. It is owned by the flRunOrch and shared by pointer with all communicators.
type FLRunHandle struct {
	// Meta contains the run's identity and configuration, used e.g. to POST setup to the app.
	Meta *FlRunMeta
	// Incoming holds messages received from the global relay server, waiting for the app to pull.
	// Can be called concurrently, has own locks
	Incoming *MessageStore
	// Outgoing holds messages posted by the app, waiting for the RelayClient to send.
	// Can be called concurrently, has own locks
	Outgoing *MessageQueue
	// IncomingNotify is signalled by the RelayClient whenever a new incoming message is stored,
	// waking the AppCommunicatorV1 incoming watcher.
	IncomingNotify chan struct{}
	// OutgoingNotify is signalled by an AppCommunicator whenever a new outgoing message is enqueued,
	// waking the RelayClient's outgoing message watcher.
	OutgoingNotify chan struct{}
	// stateLock guards state transitions and ensures cleanup waits for in-flight operations to complete.
	// Read lock: acquired during message operations (Enqueue/Pull), prevents concurrent state transitions.
	// Write lock: acquired during cleanup (MarkInactive), blocks until all readers finish.
	stateLock sync.RWMutex
	// State tracks the run's lifecycle (Init, Running, Error, Finished).
	// Shared by pointer with TCPIO and other components for automatic cascade failures.
	State atomic.Uint32
	// Tracks manual comm IDs used for each aggregator to ensure uniqueness.
	// CLIENT OUTGOING: aggregator → commID
	manualCommIDsToAggreator    map[string]map[string]struct{}
	manualCommIDsToAggregatorMu sync.Mutex

	// Tracks incoming manual comm IDs for replay/duplicate detection.
	// Used for all incoming manual comm IDs keyed by (aggregator, senderClientID, commID).
	incomingManualCommIDs   map[string]map[shared.ClientID]map[string]struct{}
	incomingManualCommIDsMu sync.Mutex

	// Tracks the latest auto comm ID counter for each aggregator.
	// As aggregator:
	// - Use the send counter to reject outdated incoming/outgoing messages
	// - Use the recv counter to ensure we never send with a counter for which we never received data
	// As client:
	// - Use the send counter to assign comm IDs and keep track of how many messages were sent
	// - Use the recv counter to reject outdated incoming/outgoing messages (comparing to the send counter)
	// The recv counter is only increased once the app actually pulls the message (SafePullNewestAuto).
	autoCommIdCounterSend map[string]uint64
	autoCommIdCounterRecv map[string]uint64
	autoCommIdMu          sync.Mutex
}

// NewFLRunHandle creates a FLRunHandle with initialized store, queue, notify channels, and state.
func NewFLRunHandle(meta *FlRunMeta) *FLRunHandle {
	h := &FLRunHandle{
		Meta:                     meta,
		Incoming:                 NewMessageStore(),
		Outgoing:                 NewSendMessageQueue(),
		IncomingNotify:           make(chan struct{}, 1),
		OutgoingNotify:           make(chan struct{}, 1),
		manualCommIDsToAggreator: make(map[string]map[string]struct{}),
		incomingManualCommIDs:    make(map[string]map[shared.ClientID]map[string]struct{}),
		autoCommIdCounterSend:    make(map[string]uint64),
		autoCommIdCounterRecv:    make(map[string]uint64),
	}
	// State defaults to StateInit (0) via zero initialization
	return h
}

// IsActive returns true if the run is still active and operational.
// A run is inactive only when it has reached a terminal state (Error or Finished).
func (h *FLRunHandle) IsActive() bool {
	if h == nil {
		return false
	}
	return enums.IsActive(h.State.Load())
}

// SafeEnqueue atomically checks if the run is active, validates the message comm ID,
// and enqueues it while holding a read lock. Returns true if enqueued successfully.
// Returns false if run is inactive or validation fails (run is marked error on failure).
// Blocks cleanup (MarkInactive) from proceeding until this operation completes.
func (h *FLRunHandle) SafeEnqueue(msg *OutgoingMessage) bool {
	if h == nil || msg == nil {
		return false
	}
	h.stateLock.RLock()
	if !h.IsActive() {
		h.stateLock.RUnlock()
		return false
	}
	if err := h.validateOutgoingMessage(msg); err != nil {
		logger.Error(BRIDGE, "", "SafeEnqueue: comm ID validation failed, marking run error: %v", err)
		h.stateLock.RUnlock()
		h.MarkError()
		return false
	}
	h.Outgoing.Enqueue(msg)
	h.stateLock.RUnlock()
	return true
}

// SafePull atomically checks if the run is active and pulls messages while holding
// a read lock. Returns the messages if successful, nil if run is inactive.
// Blocks cleanup (MarkInactive) from proceeding until this operation completes.
func (h *FLRunHandle) SafePull(communicationId *string, clientIds []shared.ClientID, fromAgg *string, toAgg *string) []IncomingMessage {
	if h == nil {
		return nil
	}

	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	if !h.IsActive() {
		return nil
	}

	return h.Incoming.Pull(communicationId, clientIds, fromAgg, toAgg)
}

// SafeDequeue atomically checks if the run is active and dequeues a message while holding
// a read lock. Returns the message and true if dequeued successfully, nil and false if run is inactive
// or queue is empty.
// Blocks cleanup (MarkInactive) from proceeding until this operation completes.
func (h *FLRunHandle) SafeDequeue() (*OutgoingMessage, bool) {
	if h == nil {
		return nil, false
	}

	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	if !h.IsActive() {
		return nil, false
	}

	return h.Outgoing.Dequeue()
}

// SafeIncomingStore atomically checks if the run is active, validates the incoming message,
// and stores it while holding a read lock. Returns true if the run is active and the message
// was either stored or intentionally discarded (stale). Returns false if run is inactive or
// validation fails (run is marked error on failure).
// Blocks cleanup (MarkInactive) from proceeding until this operation completes.
func (h *FLRunHandle) SafeIncomingStore(msg IncomingMessage) bool {
	if h == nil {
		return false
	}
	h.stateLock.RLock()
	if !h.IsActive() {
		h.stateLock.RUnlock()
		return false
	}
	discard, err := h.validateIncomingMessage(msg)
	if err != nil {
		logger.Error(BRIDGE, "", "SafeIncomingStore: comm ID validation failed, marking run error: %v", err)
		h.stateLock.RUnlock()
		h.MarkError()
		return false
	}
	if discard {
		h.stateLock.RUnlock()
		return true
	}
	h.Incoming.Store(msg)
	h.stateLock.RUnlock()
	return true
}

// SafePullNewestAuto atomically checks if the run is active and returns the single newest
// auto-comm-ID message matching the given fromAgg/clientIds filters, discarding all older
// auto messages for those filters. On a successful pull it also advances the send and recv
// counters to the pulled message's counter (logging a warning if rounds were skipped).
// Returns nil if run is inactive or no messages match.
func (h *FLRunHandle) SafePullNewestAuto(fromAgg *string, clientIds []shared.ClientID) *IncomingMessage {
	if h == nil {
		return nil
	}

	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	if !h.IsActive() {
		return nil
	}

	msg := h.Incoming.PullNewestAuto(fromAgg, clientIds)
	if msg != nil && fromAgg != nil && strings.HasPrefix(msg.CommunicationId, models.AutoCommIDPrefix) {
		suffix := strings.TrimPrefix(msg.CommunicationId, models.AutoCommIDPrefix)
		if counter, err := strconv.ParseUint(suffix, 10, 64); err == nil {
			h.autoCommIdMu.Lock()
			if counter > h.autoCommIdCounterRecv[*fromAgg] {
				prev := h.autoCommIdCounterSend[*fromAgg]
				if counter > prev+1 {
					logger.Warn(BRIDGE, "", "SafePullNewestAuto: advancing counter from %d to %d for aggregator %q (skipped rounds)", prev, counter, *fromAgg)
				}
				h.autoCommIdCounterRecv[*fromAgg] = counter
				h.autoCommIdCounterSend[*fromAgg] = counter
			}
			h.autoCommIdMu.Unlock()
		}
	}
	return msg
}

// SafePullGroupedByCommID atomically checks if the run is active and pulls messages grouped
// by comm ID, returning only groups with at least minPackages messages. Returns nil if the
// run is inactive (caller should return 409), or an empty map if no complete groups exist yet.
func (h *FLRunHandle) SafePullGroupedByCommID(minPackages int, commID *string, toAgg *string, clientIds []shared.ClientID) map[string][]IncomingMessage {
	if h == nil {
		return nil
	}

	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	if !h.IsActive() {
		return nil
	}

	return h.Incoming.PullGroupedByCommID(minPackages, commID, toAgg, clientIds)
}

// GetState returns the current state of the FL run.
func (h *FLRunHandle) GetState() uint32 {
	if h == nil {
		return enums.StateInit
	}
	return h.State.Load()
}

// MarkRunning transitions the run from StateInit to StateRunning.
// Returns false if the run is not currently in StateInit.
func (h *FLRunHandle) MarkRunning() bool {
	if h == nil {
		return false
	}
	return h.State.CompareAndSwap(enums.StateInit, enums.StateRunning)
}

// markTerminalState is a helper that transitions the run to a terminal state (Error or Finished).
// It acquires the write lock, blocking all in-flight operations until they complete.
// Once the lock is acquired, it counts messages in the store and queue, clears them,
// transitions to the target state, and returns the counts.
// Returns (success, incomingDiscarded, outgoingDiscarded).
// success is false if the run is already in a terminal state.
func (h *FLRunHandle) markTerminalState(targetState uint32) (bool, int, int) {
	if h == nil {
		return false, 0, 0
	}

	// Acquire write lock - blocks until all in-flight Safe* operations complete
	h.stateLock.Lock()
	defer h.stateLock.Unlock()

	// Check if already in terminal state
	current := h.State.Load()
	if current == enums.StateFinished || current == enums.StateError {
		return false, 0, 0 // Already terminal, no-op
	}

	// Count messages to discard before clearing
	incomingCount := h.Incoming.Count()
	outgoingCount := h.Outgoing.Len()

	// Clear the message stores (set to nil so no new operations access stale data)
	h.Incoming = nil
	h.Outgoing = nil

	// Transition to target state
	h.State.Store(targetState)

	// Wake any goroutine blocked on IncomingNotify or OutgoingNotify so it can
	// observe the terminal state and exit. Non-blocking: if the channel already
	// holds a pending signal the watcher will wake on that signal and see inactive.
	select {
	case h.IncomingNotify <- struct{}{}:
	default:
	}
	select {
	case h.OutgoingNotify <- struct{}{}:
	default:
	}

	return true, incomingCount, outgoingCount
}

// MarkError transitions the run to StateError, acquires the write lock to block in-flight operations,
// and clears the message stores. Returns (success, incomingDiscarded, outgoingDiscarded).
func (h *FLRunHandle) MarkError() (bool, int, int) {
	return h.markTerminalState(enums.StateError)
}

// MarkFinished transitions the run to StateFinished, acquires the write lock to block in-flight operations,
// and clears the message stores. Returns (success, incomingDiscarded, outgoingDiscarded).
func (h *FLRunHandle) MarkFinished() (bool, int, int) {
	return h.markTerminalState(enums.StateFinished)
}

// IsFinished returns true if the run has reached StateFinished.
func (h *FLRunHandle) IsFinished() bool {
	if h == nil {
		return false
	}
	return h.State.Load() == enums.StateFinished
}

// ── OUTGOING COMM ID VALIDATION ───────────────────────────────────────────────

// validateOutgoingMessage validates the comm ID of an outgoing message and, for client→agg
// auto comm ID messages, assigns the next counter. Must be called while holding stateLock.RLock.
func (h *FLRunHandle) validateOutgoingMessage(msg *OutgoingMessage) error {
	commId := string(msg.Memo[:msg.MemoSize])
	isPeerToPeer := msg.ToAggregatorNameSize == 0 && !h.Meta.IsCoordinator

	if isPeerToPeer {
		if strings.HasPrefix(commId, models.AutoCommIDPrefix) {
			return fmt.Errorf("auto comm ID prefix not allowed in peer to peer messages")
		}
		if commId == models.RequestAutoCommId {
			return fmt.Errorf("%s not allowed in peer to peer messages", models.RequestAutoCommId)
		}
		return nil
	}

	if h.Meta.IsCoordinator {
		// Coordinator → clients broadcast
		if commId == models.RequestAutoCommId {
			return fmt.Errorf("%s cannot be used when broadcasting, use the actual comm ID you aggregated for", models.RequestAutoCommId)
		}
		if strings.HasPrefix(commId, models.AutoCommIDPrefix) {
			aggregatorName := string(msg.FromAggregatorName[:msg.FromAggregatorNameSize])
			suffix := strings.TrimPrefix(commId, models.AutoCommIDPrefix)
			counter, err := strconv.ParseUint(suffix, 10, 64)
			if err != nil {
				return fmt.Errorf("malformed auto comm ID %q: %w", commId, err)
			}
			h.autoCommIdMu.Lock()
			defer h.autoCommIdMu.Unlock()
			if counter <= h.autoCommIdCounterSend[aggregatorName] {
				return fmt.Errorf("auto comm ID %q not greater than current send counter %d for aggregator %q, sending an outdated message", commId, h.autoCommIdCounterSend[aggregatorName], aggregatorName)
			}
			if counter > h.autoCommIdCounterRecv[aggregatorName] {
				return fmt.Errorf("auto comm ID %q exceeds recv counter %d for aggregator %q — no data received for this round but trying to send data for it", commId, h.autoCommIdCounterRecv[aggregatorName], aggregatorName)
			}
			h.autoCommIdCounterSend[aggregatorName] = counter
		}
		return nil
	}

	// Client → aggregator
	aggregatorName := string(msg.ToAggregatorName[:msg.ToAggregatorNameSize])
	if msg.MemoSize == 0 || commId == models.RequestAutoCommId {
		return h.assignAutoCommId(aggregatorName, msg)
	}
	return h.CheckAndRecordOutgoingManualCommIDToAggregator(aggregatorName, commId)
}

// assignAutoCommId increments the send counter for aggregatorName and writes the resulting
// auto comm ID into msg. Returns an error (run will be failed) if recvCounter has caught up
// to the new sendCounter — this is a safety net for the rare race where SafePullNewestAuto
// advanced the counters between the app's enqueue call and this check.
// Caller must hold stateLock.RLock (not autoCommIdMu — acquired here).
func (h *FLRunHandle) assignAutoCommId(aggregatorName string, msg *OutgoingMessage) error {
	h.autoCommIdMu.Lock()
	defer h.autoCommIdMu.Unlock()
	h.autoCommIdCounterSend[aggregatorName]++
	send := h.autoCommIdCounterSend[aggregatorName]
	recv := h.autoCommIdCounterRecv[aggregatorName]
	if recv >= send {
		return fmt.Errorf("send counter %d lagging behind recv counter %d for aggregator %q, discarding outgoing message as outdated", send, recv, aggregatorName)
	}
	commId := models.AutoCommIDPrefix + strconv.FormatUint(send, 10)
	msg.MemoSize = byte(len(commId))
	msg.Memo = []byte(commId)
	return nil
}

// CheckAndRecordOutgoingManualCommIDToAggregator validates and records a manual outgoing
// comm ID for the given aggregator. Returns an error if commID starts with the reserved
// auto prefix or has already been used for this aggregator.
func (h *FLRunHandle) CheckAndRecordOutgoingManualCommIDToAggregator(aggregator, commID string) error {
	if strings.HasPrefix(commID, models.AutoCommIDPrefix) {
		return fmt.Errorf("invalid comm ID: starts with reserved prefix")
	}
	h.manualCommIDsToAggregatorMu.Lock()
	defer h.manualCommIDsToAggregatorMu.Unlock()
	if h.manualCommIDsToAggreator[aggregator] == nil {
		h.manualCommIDsToAggreator[aggregator] = make(map[string]struct{})
	}
	if _, exists := h.manualCommIDsToAggreator[aggregator][commID]; exists {
		return fmt.Errorf("invalid comm ID: already used")
	}
	h.manualCommIDsToAggreator[aggregator][commID] = struct{}{}
	return nil
}

// ── INCOMING COMM ID VALIDATION ───────────────────────────────────────────────

// validateIncomingMessage validates the comm ID of an incoming message.
// Returns (discard=true, nil) for stale auto comm IDs that should be silently dropped.
// Returns (false, error) for protocol violations that should fail the run.
// Must be called while holding stateLock.RLock.
func (h *FLRunHandle) validateIncomingMessage(msg IncomingMessage) (discard bool, err error) {
	commId := msg.CommunicationId
	fromCoordinator := msg.FromClientId == h.Meta.CoordinatorID
	// Coordinator broadcasts have ToAggregatorName="" but are NOT peer-to-peer.
	// Only messages from non-coordinator clients with no aggregator target are p2p.
	isPeerToPeer := msg.ToAggregatorName == "" && !fromCoordinator

	if isPeerToPeer {
		if strings.HasPrefix(commId, models.AutoCommIDPrefix) || commId == models.RequestAutoCommId {
			return false, fmt.Errorf("invalid comm ID %q in peer to peer message", commId)
		}
		return false, nil // duplicates allowed in p2p
	}

	if fromCoordinator {
		// Coordinator → client: client is receiving a broadcast.
		// Aggregator identity is in FromAggregatorName (ToAggregatorName is empty on broadcasts).
		aggregatorName := msg.FromAggregatorName
		if !strings.HasPrefix(commId, models.AutoCommIDPrefix) {
			return false, h.checkIncomingManualCommID(aggregatorName, msg.FromClientId, commId)
		}
		// Auto: discard if counter < sendCounter (strictly less — receiving aggregation of current round is valid)
		suffix := strings.TrimPrefix(commId, models.AutoCommIDPrefix)
		counter, err := strconv.ParseUint(suffix, 10, 64)
		if err != nil {
			return false, fmt.Errorf("malformed auto comm ID %q: %w", commId, err)
		}
		h.autoCommIdMu.Lock()
		defer h.autoCommIdMu.Unlock()
		if counter < h.autoCommIdCounterSend[aggregatorName] {
			logger.Warn(BRIDGE, "", "Discarding stale incoming auto comm ID %q for aggregator %q (sendCounter=%d)", commId, aggregatorName, h.autoCommIdCounterSend[aggregatorName])
			return true, nil
		}
		// recvCounter is updated by SafePullNewestAuto when the app retrieves the message
		return false, nil
	}

	// Client → aggregator: coordinator is receiving
	aggregatorName := msg.ToAggregatorName
	if !strings.HasPrefix(commId, models.AutoCommIDPrefix) {
		return false, h.checkIncomingManualCommID(aggregatorName, msg.FromClientId, commId)
	}
	// Auto: discard if counter <= sendCounter (coordinator already broadcast this round or later)
	suffix := strings.TrimPrefix(commId, models.AutoCommIDPrefix)
	counter, err := strconv.ParseUint(suffix, 10, 64)
	if err != nil {
		return false, fmt.Errorf("malformed auto comm ID %q: %w", commId, err)
	}
	h.autoCommIdMu.Lock()
	defer h.autoCommIdMu.Unlock()
	if counter <= h.autoCommIdCounterSend[aggregatorName] {
		logger.Warn(BRIDGE, "", "Discarding stale incoming auto comm ID %q from client %s for aggregator %q (sendCounter=%d)", commId, msg.FromClientId.ToString(), aggregatorName, h.autoCommIdCounterSend[aggregatorName])
		return true, nil
	}
	if counter > h.autoCommIdCounterRecv[aggregatorName] {
		h.autoCommIdCounterRecv[aggregatorName] = counter
	}
	return false, nil
}

// checkIncomingManualCommID checks that the incoming manual comm ID has not been seen before
// for this (aggregator, senderClientID) pair and records it. Returns an error if commId uses
// a reserved prefix or has already been received.
func (h *FLRunHandle) checkIncomingManualCommID(aggregatorName string, clientId shared.ClientID, commId string) error {
	if strings.HasPrefix(commId, models.AutoCommIDPrefix) || commId == models.RequestAutoCommId {
		return fmt.Errorf("reserved comm ID %q not allowed in manual message", commId)
	}
	h.incomingManualCommIDsMu.Lock()
	defer h.incomingManualCommIDsMu.Unlock()
	if h.incomingManualCommIDs[aggregatorName] == nil {
		h.incomingManualCommIDs[aggregatorName] = make(map[shared.ClientID]map[string]struct{})
	}
	if h.incomingManualCommIDs[aggregatorName][clientId] == nil {
		h.incomingManualCommIDs[aggregatorName][clientId] = make(map[string]struct{})
	}
	if _, exists := h.incomingManualCommIDs[aggregatorName][clientId][commId]; exists {
		return fmt.Errorf("reused comm ID %q from client %s to aggregator %s", commId, clientId.ToString(), aggregatorName)
	}
	h.incomingManualCommIDs[aggregatorName][clientId][commId] = struct{}{}
	return nil
}
