// Helper message queue used to cache outgoing data messages towards other
// clients/the aggregator. Meant for the app communication to push messages to and
// another watcher routine to pull and send messages.
// Is threadsafe so can be used by multiple goroutines at the same time
// e.g. multiple receive data requests and the relay server pushing messages at the same time
// IMPORTANT:
// You enqueue pointers here. DO NOT CHANGE THEM AFTER ENQUEUEING.
package bridge

import (
	enums "fc_controller/pkg/controller/enums"
	models "fc_controller/pkg/controller/models"
	shared "fc_controller/pkg/shared/link"
	"sync"
	"time"
)

// OutgoingMessage contains all metadata and payload bytes required to send one packet.
// These fields mirror the parameters used by the relay writer.
type OutgoingMessage struct {
	DestinationClientID *shared.ClientID
	TimestampArrived    time.Time

	// SrcSerializationMode is how the app serialized the payload — used only to
	// deserialize before DP/SMPC processing. Never used for the wire header.
	SrcSerializationMode     enums.SerializationMode
	RequestedCompressionMode enums.CompressionMode

	MemoSize byte
	Memo     []byte

	ToAggregatorNameSize   byte
	ToAggregatorName       []byte
	FromAggregatorNameSize byte
	FromAggregatorName     []byte

	SmpcProps models.NormalizedSMPCProperties
	DpProps   models.NormalizedDPProperties

	Payload []byte
}

// MessageQueue is a FIFO queue used for outbound packets.
type MessageQueue struct {
	lock sync.Mutex
	data []*OutgoingMessage
}

func NewSendMessageQueue() *MessageQueue {
	return &MessageQueue{
		lock: sync.Mutex{},
		data: make([]*OutgoingMessage, 0),
	}
}

// Push appends one message to the queue tail.
func (q *MessageQueue) Enqueue(m *OutgoingMessage) {
	if m == nil {
		return
	}
	q.lock.Lock()
	defer q.lock.Unlock()

	q.data = append(q.data, m)
}

// Dequeue removes and returns one message from the queue head.
func (q *MessageQueue) Dequeue() (*OutgoingMessage, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.data) == 0 {
		return nil, false
	}

	msg := q.data[0]
	q.data = q.data[1:]
	return msg, true
}

func (q *MessageQueue) Len() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return len(q.data)
}
