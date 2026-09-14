package bridge

import (
	"bytes"
	"testing"

	shared "fc_controller/pkg/shared/link"
)

func TestSendMessageQueue_PushDequeue_FIFO(t *testing.T) {
	q := NewSendMessageQueue()

	msg1 := &OutgoingMessage{Payload: []byte("m1")}
	msg2 := &OutgoingMessage{Payload: []byte("m2")}
	q.Enqueue(msg1)
	q.Enqueue(msg2)

	got1, ok := q.Dequeue()
	if !ok {
		t.Fatal("expected first message")
	}
	if !bytes.Equal(got1.Payload, []byte("m1")) {
		t.Fatalf("unexpected first message: data=%s", string(got1.Payload))
	}

	got2, ok := q.Dequeue()
	if !ok {
		t.Fatal("expected second message")
	}
	if !bytes.Equal(got2.Payload, []byte("m2")) {
		t.Fatalf("unexpected second message: data=%s", string(got2.Payload))
	}

	_, ok = q.Dequeue()
	if ok {
		t.Fatal("expected queue to be empty")
	}
}

func TestSendMessageQueue_Push_StoresPointerWithoutCopy(t *testing.T) {
	q := NewSendMessageQueue()
	dest := shared.ClientID{1, 2, 3, 4, 5, 6, 7, 8}
	memo := []byte("memo")
	payload := []byte("payload")

	msg := &OutgoingMessage{
		DestinationClientID: &dest,
		Memo:                memo,
		Payload:             payload,
	}
	q.Enqueue(msg)

	// Mutate source buffers after enqueueing. Queue stores the same pointers.
	dest[0] = 9
	memo[0] = 'x'
	payload[0] = 'X'

	got, ok := q.Dequeue()
	if !ok {
		t.Fatal("expected queued message")
	}
	if got.DestinationClientID == nil {
		t.Fatal("expected destination to be present")
	}
	if got.DestinationClientID[0] != 9 {
		t.Fatalf("expected queued pointer destination update, got first byte %d", got.DestinationClientID[0])
	}
	if !bytes.Equal(got.Memo, []byte("xemo")) {
		t.Fatalf("expected queued pointer memo update, got %s", string(got.Memo))
	}
	if !bytes.Equal(got.Payload, []byte("Xayload")) {
		t.Fatalf("expected queued pointer payload update, got %s", string(got.Payload))
	}
}
