// Helper message store used to cache incoming data messages from other
// clients/the aggregator and make them available for retrieval.
// This store can be queried efficiently O(1) by
// - communicationId
// - clientId
// - communicationId + clientId
// Is threadsafe so can be used by multiple goroutines at the same time
// e.g. multiple receive data requests and the relay server pushing messages at the same time
package bridge

import (
	"cmp"
	"fc_controller/pkg/controller/enums"
	"fc_controller/pkg/controller/models"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const STORE = "GLOBAL_TO_APP_MESSAGE_STORE"

type messageKey struct {
	commID   string
	clientID shared.ClientID
	fromAgg  string
	toAgg    string
}

type IncomingMessage struct {
	CommunicationId          string
	FromClientId             shared.ClientID
	SrcSerializationModeByte enums.SerializationModeByte
	SrcCompressionModeByte   enums.CompressionModeByte
	ToAggregatorName         string
	FromAggregatorName       string
	TimestampArrived         time.Time
	Data                     interface{}
}

func NewIncomingMessage(srcSerializationModeByte enums.SerializationModeByte, srcCompressionModeByte enums.CompressionModeByte, header *shared.DataPacketHeader, data interface{}) IncomingMessage {
	// Not taken from header:
	// - SrcSerializationModeByte and SrcCompressionModeByte: The message might have been processed and not be in the original serialization/compression specified by the header anymore
	// - TimestampArrived: Set on arrival to the store, not by the sender
	return IncomingMessage{
		CommunicationId:          header.MemoString,
		FromClientId:             header.FromClientID,
		SrcSerializationModeByte: srcSerializationModeByte,
		SrcCompressionModeByte:   srcCompressionModeByte,
		ToAggregatorName:         header.ToAggregatorName,
		FromAggregatorName:       header.FromAggregatorName,
		TimestampArrived:         time.Now(),
		Data:                     data,
	}
}

type incomingMessageWrapper struct {
	message IncomingMessage
	// backpointers to the keys of the message for cleanup
	idxs     []messageKey // entries in idx (explicit comm ID index)
	autoIdxs []messageKey // entries in autoIdx (automatic comm ID index, keyed without commID)
}

type MessageStore struct {
	nextID  int64
	lock    sync.RWMutex
	store   map[int64]incomingMessageWrapper
	idx     map[messageKey]map[int64]struct{} // idx -> set of message IDs (explicit comm IDs)
	autoIdx map[messageKey]map[int64]struct{} // idx -> set of message IDs (automatic comm IDs, commID omitted from key)
}

func NewMessageStore() *MessageStore {
	return &MessageStore{
		nextID:  0,
		lock:    sync.RWMutex{},
		store:   make(map[int64]incomingMessageWrapper),
		idx:     make(map[messageKey]map[int64]struct{}),
		autoIdx: make(map[messageKey]map[int64]struct{}),
	}
}

func addToIndex(index map[messageKey]map[int64]struct{}, key messageKey, id int64) {
	if _, exists := index[key]; !exists {
		index[key] = make(map[int64]struct{})
	}
	index[key][id] = struct{}{}
}

func removeFromIndex(index map[messageKey]map[int64]struct{}, keys []messageKey, id int64) {
	for _, k := range keys {
		if bucket, exists := index[k]; exists {
			delete(bucket, id)
			if len(bucket) == 0 {
				delete(index, k)
			}
		}
	}
}

// Push adds the message (thread safe) to the store and indexes it correctly
func (q *MessageStore) Store(m IncomingMessage) {
	q.lock.Lock()
	defer q.lock.Unlock()

	id := q.nextID
	q.nextID++

	// Index entries for non-empty comm, non-zero client, and their combination.
	// Empty keys are skipped — Pull can't filter by them anyway.
	hasComm := m.CommunicationId != ""
	hasClient := m.FromClientId != shared.ZERO_CLIENT_ID
	hasFromAgg := m.FromAggregatorName != ""
	hasToAgg := m.ToAggregatorName != ""
	// ToAgg and FromAgg cannot be combined in an index
	idxs := make([]messageKey, 0, 11)
	// Single index (4)
	if hasComm {
		idxs = append(idxs, messageKey{commID: m.CommunicationId})
	}
	if hasClient {
		idxs = append(idxs, messageKey{clientID: m.FromClientId})
	}
	if hasFromAgg {
		idxs = append(idxs, messageKey{fromAgg: m.FromAggregatorName})
	}
	if hasToAgg {
		idxs = append(idxs, messageKey{toAgg: m.ToAggregatorName})
	}
	// Double index (5)
	if hasComm && hasClient {
		idxs = append(idxs, messageKey{commID: m.CommunicationId, clientID: m.FromClientId})
	}

	if hasFromAgg && hasComm {
		idxs = append(idxs, messageKey{commID: m.CommunicationId, fromAgg: m.FromAggregatorName})
	}
	if hasFromAgg && hasClient {
		idxs = append(idxs, messageKey{clientID: m.FromClientId, fromAgg: m.FromAggregatorName})
	}
	if hasToAgg && hasComm {
		idxs = append(idxs, messageKey{commID: m.CommunicationId, toAgg: m.ToAggregatorName})
	}
	if hasToAgg && hasClient {
		idxs = append(idxs, messageKey{clientID: m.FromClientId, toAgg: m.ToAggregatorName})
	}
	// Triple index (2)
	if hasComm && hasClient && hasFromAgg {
		idxs = append(idxs, messageKey{commID: m.CommunicationId, clientID: m.FromClientId, fromAgg: m.FromAggregatorName})
	}
	if hasComm && hasClient && hasToAgg {
		idxs = append(idxs, messageKey{commID: m.CommunicationId, clientID: m.FromClientId, toAgg: m.ToAggregatorName})
	}

	for _, k := range idxs {
		addToIndex(q.idx, k, id)
	}

	// For automatic comm IDs, also index in autoIdx — same combinations but commID omitted,
	// so Pull(commID="") can retrieve all automatic messages with full filter support.
	var autoIdxs []messageKey
	if strings.HasPrefix(m.CommunicationId, models.AutoCommIDPrefix) {
		autoIdxs = make([]messageKey, 0, 6)
		autoIdxs = append(autoIdxs, messageKey{}) // catch-all: all automatic messages
		// Single Index (3, no comm id)
		if hasClient {
			autoIdxs = append(autoIdxs, messageKey{clientID: m.FromClientId})
		}
		if hasFromAgg {
			autoIdxs = append(autoIdxs, messageKey{fromAgg: m.FromAggregatorName})
		}
		if hasToAgg {
			autoIdxs = append(autoIdxs, messageKey{toAgg: m.ToAggregatorName})
		}

		// Double index (2, no commId combinations)
		if hasClient && hasFromAgg {
			autoIdxs = append(autoIdxs, messageKey{clientID: m.FromClientId, fromAgg: m.FromAggregatorName})
		}
		if hasClient && hasToAgg {
			autoIdxs = append(autoIdxs, messageKey{clientID: m.FromClientId, toAgg: m.ToAggregatorName})
		}
		for _, k := range autoIdxs {
			addToIndex(q.autoIdx, k, id)
		}
		// No tripple index as no comm id
	}

	q.store[id] = incomingMessageWrapper{message: m, idxs: idxs, autoIdxs: autoIdxs}
}

// Pull receives a message from the store (thread safe) and removes it from the store and all indexes.
// returns messages sorted from oldest to newest
// Sorts by arrival arrival time
// Special behaviour:
// If the commId is models.RequestAutoCommId:
//   - pulls all messages with an automatic comm id
//   - sorts them from lowest auto comm ID to highest, using arrival time only
//     if messages have the same auto comm id
func (q *MessageStore) Pull(commID *string, clientIds []shared.ClientID, fromAgg *string, toAgg *string) []IncomingMessage {
	q.lock.Lock()
	defer q.lock.Unlock()

	// There are two ways to filter by comm ID:
	// commId == models.RequestAutoCommId -> give back all messages using an automatic comm id
	// and fullfilling the other filters
	// ==> isAutoFilter == True
	// commId == any other non empty string
	// filter for that manual comm id
	// ==> manualCommFilter == True
	// Either one of these filter or neither can be applied but never both together
	isAutoFilter := commID != nil && *commID == models.RequestAutoCommId
	manualCommFilter := commID != nil && *commID != "" && !isAutoFilter

	clientFilterUsed := len(clientIds) > 0
	fromAggFilterUsed := fromAgg != nil && *fromAgg != ""
	toAggFilterUsed := toAgg != nil && *toAgg != ""

	if fromAggFilterUsed && toAggFilterUsed {
		logger.Error(STORE, "", "Cannot filter by both fromAgg and toAgg at the same time")
		return []IncomingMessage{}
	}

	// No filter: return and purge everything
	if !isAutoFilter && !manualCommFilter && !clientFilterUsed && !fromAggFilterUsed && !toAggFilterUsed {
		messages := make([]IncomingMessage, 0, len(q.store))
		for _, msg := range q.store {
			messages = append(messages, msg.message)
		}
		q.store = make(map[int64]incomingMessageWrapper)
		q.idx = make(map[messageKey]map[int64]struct{})
		q.autoIdx = make(map[messageKey]map[int64]struct{})
		q.OrderByArrivalTime(messages)
		return messages
	}

	// Choose which index to look up in based on whether this is an auto-filter.
	// For auto-filter: use autoIdx with commID omitted from the key.
	// For explicit filter: use idx with commID in the key.
	lookupIndex := q.idx
	if isAutoFilter {
		lookupIndex = q.autoIdx
	}

	baseKey := messageKey{}
	if manualCommFilter && !isAutoFilter {
		baseKey.commID = *commID
	}
	if fromAggFilterUsed {
		baseKey.fromAgg = *fromAgg
	}
	if toAggFilterUsed {
		baseKey.toAgg = *toAgg
	}

	var keys []messageKey
	if clientFilterUsed {
		keys = make([]messageKey, 0, len(clientIds))
		for _, clientID := range clientIds {
			k := baseKey
			k.clientID = clientID
			keys = append(keys, k)
		}
	} else {
		keys = []messageKey{baseKey}
	}

	var messageIds []int64
	for _, k := range keys {
		if ids, exists := lookupIndex[k]; exists {
			for id := range ids {
				messageIds = append(messageIds, id)
			}
		}
	}

	messages := make([]IncomingMessage, 0, len(messageIds))
	for _, msgID := range messageIds {
		msg, ok := q.store[msgID]
		if !ok {
			logger.Error(STORE, "", "Index out of sync")
			continue
		}
		messages = append(messages, msg.message)
		removeFromIndex(q.idx, msg.idxs, msgID)
		removeFromIndex(q.autoIdx, msg.autoIdxs, msgID)
		delete(q.store, msgID)
	}
	if isAutoFilter {
		q.OrderByAutoCommIDCounter(messages)
	} else {
		q.OrderByArrivalTime(messages)
	}
	return messages
}

// OrderByArrivalTime sorts a slice of messages by arrival time, oldest first.
func (q *MessageStore) OrderByArrivalTime(messages []IncomingMessage) {
	slices.SortStableFunc(messages, func(a, b IncomingMessage) int {
		return a.TimestampArrived.Compare(b.TimestampArrived)
	})
}

// OrderByAutoCommIDCounter sorts messages by the numeric counter embedded in their automatic comm ID.
// Required because lexicographic order breaks at N≥10 ("AUTOMATIC_COMM_ID_9" > "AUTOMATIC_COMM_ID_10").
func (q *MessageStore) OrderByAutoCommIDCounter(messages []IncomingMessage) {
	slices.SortStableFunc(messages, func(a, b IncomingMessage) int {
		na, _ := strconv.ParseUint(strings.TrimPrefix(a.CommunicationId, models.AutoCommIDPrefix), 10, 64)
		nb, _ := strconv.ParseUint(strings.TrimPrefix(b.CommunicationId, models.AutoCommIDPrefix), 10, 64)
		return cmp.Compare(na, nb)
	})
}

// PullNewestAuto returns the single newest auto-comm-ID message matching the given fromAgg and
// clientIds filters. ALL matching auto messages are consumed (removed from the store) regardless
// of age — older rounds are discarded so the caller is always in the most up-to-date round.
// Returns nil if no matching auto messages exist.
func (q *MessageStore) PullNewestAuto(fromAgg *string, clientIds []shared.ClientID) *IncomingMessage {
	q.lock.Lock()
	defer q.lock.Unlock()

	fromAggFilterUsed := fromAgg != nil && *fromAgg != ""
	clientFilterUsed := len(clientIds) > 0

	baseKey := messageKey{}
	if fromAggFilterUsed {
		baseKey.fromAgg = *fromAgg
	}

	var keys []messageKey
	if clientFilterUsed {
		keys = make([]messageKey, 0, len(clientIds))
		for _, clientID := range clientIds {
			k := baseKey
			k.clientID = clientID
			keys = append(keys, k)
		}
	} else {
		keys = []messageKey{baseKey}
	}

	var messageIds []int64
	for _, k := range keys {
		if ids, exists := q.autoIdx[k]; exists {
			for id := range ids {
				messageIds = append(messageIds, id)
			}
		}
	}
	if len(messageIds) == 0 {
		return nil
	}

	// Collect and remove ALL matching auto messages (discard old rounds).
	messages := make([]IncomingMessage, 0, len(messageIds))
	for _, msgID := range messageIds {
		wrapper, ok := q.store[msgID]
		if !ok {
			logger.Error(STORE, "", "Index out of sync in PullNewestAuto")
			continue
		}
		messages = append(messages, wrapper.message)
		removeFromIndex(q.idx, wrapper.idxs, msgID)
		removeFromIndex(q.autoIdx, wrapper.autoIdxs, msgID)
		delete(q.store, msgID)
	}
	if len(messages) == 0 {
		return nil
	}

	q.OrderByAutoCommIDCounter(messages)
	newest := messages[len(messages)-1]
	return &newest
}

// PullGroupedByCommID pulls messages grouped by comm ID, returning only groups with at least
// minPackages messages. Filters (commID, toAgg, clientIds) mirror Pull semantics:
//   - commID nil or ""     → all comm IDs (manual and auto); full store scan with toAgg/clientIds filters
//   - commID "#AUTOMATIC"  → auto-ID messages only (uses autoIdx)
//   - any other string     → that specific comm ID (uses idx); result map has at most one key
//
// All messages in qualifying groups are consumed. Returns map[commID][]IncomingMessage.
// Returns an empty map (not nil) when no complete groups exist yet.
func (q *MessageStore) PullGroupedByCommID(minPackages int, commID *string, toAgg *string, clientIds []shared.ClientID) map[string][]IncomingMessage {
	q.lock.Lock()
	defer q.lock.Unlock()

	isAutoFilter := commID != nil && *commID == models.RequestAutoCommId
	manualCommFilter := commID != nil && *commID != "" && !isAutoFilter
	noCommFilter := !isAutoFilter && !manualCommFilter

	toAggFilterUsed := toAgg != nil && *toAgg != ""
	clientFilterUsed := len(clientIds) > 0

	var candidateIDs []int64

	if noCommFilter {
		// Full store scan — apply toAgg and clientIds filters manually.
		for id, wrapper := range q.store {
			msg := wrapper.message
			if toAggFilterUsed && msg.ToAggregatorName != *toAgg {
				continue
			}
			if clientFilterUsed {
				found := false
				for _, cid := range clientIds {
					if msg.FromClientId == cid {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			candidateIDs = append(candidateIDs, id)
		}
	} else {
		lookupIndex := q.idx
		if isAutoFilter {
			lookupIndex = q.autoIdx
		}
		baseKey := messageKey{}
		if manualCommFilter {
			baseKey.commID = *commID
		}
		if toAggFilterUsed {
			baseKey.toAgg = *toAgg
		}
		var keys []messageKey
		if clientFilterUsed {
			keys = make([]messageKey, 0, len(clientIds))
			for _, clientID := range clientIds {
				k := baseKey
				k.clientID = clientID
				keys = append(keys, k)
			}
		} else {
			keys = []messageKey{baseKey}
		}
		for _, k := range keys {
			if ids, exists := lookupIndex[k]; exists {
				for id := range ids {
					candidateIDs = append(candidateIDs, id)
				}
			}
		}
	}

	// Group candidate message IDs by comm ID.
	groups := make(map[string][]int64)
	for _, id := range candidateIDs {
		wrapper, ok := q.store[id]
		if !ok {
			continue
		}
		commIDStr := wrapper.message.CommunicationId
		groups[commIDStr] = append(groups[commIDStr], id)
	}

	// Consume and return only groups with >= minPackages messages.
	result := make(map[string][]IncomingMessage)
	for commIDStr, ids := range groups {
		if len(ids) < minPackages {
			continue
		}
		messages := make([]IncomingMessage, 0, len(ids))
		for _, id := range ids {
			wrapper, ok := q.store[id]
			if !ok {
				logger.Error(STORE, "", "Index out of sync in PullGroupedByCommID")
				continue
			}
			messages = append(messages, wrapper.message)
			removeFromIndex(q.idx, wrapper.idxs, id)
			removeFromIndex(q.autoIdx, wrapper.autoIdxs, id)
			delete(q.store, id)
		}
		q.OrderByArrivalTime(messages)
		result[commIDStr] = messages
	}
	return result
}

// Count returns the number of messages currently in the store.
func (q *MessageStore) Count() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.store)
}
