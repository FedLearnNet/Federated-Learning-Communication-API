package bridge

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"fc_controller/pkg/controller/enums"
	shared "fc_controller/pkg/shared/link"
)

func makeClientID(b byte) shared.ClientID {
	id := shared.ClientID{}
	id[0] = b
	return id
}

func TestMessageQueue_PullBySpecificIndex_CleansAllIndexes(t *testing.T) {
	q := NewMessageStore()

	m := IncomingMessage{
		CommunicationId: "comm-1",
		FromClientId:    makeClientID(1),
		Data:            []byte("payload-1"),
	}

	q.Store(m)

	got := q.Pull(strPtr("comm-1"), nil, nil, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	if !bytes.Equal(got[0].Data.([]byte), []byte("payload-1")) {
		t.Fatalf("expected payload-1, got %s", got[0].Data)
	}

	// Ensure message was removed from all other indexes, not only queried index.
	if again := q.Pull(nil, nil, nil, nil); len(again) != 0 {
		t.Fatalf("expected 0 messages after cleanup, got %d", len(again))
	}
	clientID1 := makeClientID(1)
	if again := q.Pull(nil, []shared.ClientID{clientID1}, nil, nil); len(again) != 0 {
		t.Fatalf("expected 0 messages on client index after cleanup, got %d", len(again))
	}
	if again := q.Pull(strPtr("comm-1"), []shared.ClientID{clientID1}, nil, nil); len(again) != 0 {
		t.Fatalf("expected 0 messages on comm+client index after cleanup, got %d", len(again))
	}
}

func TestMessageQueue_PullAll_ReturnsAllMatchingMessagesOnce(t *testing.T) {
	q := NewMessageStore()

	q.Store(IncomingMessage{CommunicationId: "comm-1", FromClientId: makeClientID(1), Data: []byte("m1"), ToAggregatorName: "to_agg1"})
	q.Store(IncomingMessage{CommunicationId: "comm-2", FromClientId: makeClientID(2), Data: []byte("m2"), ToAggregatorName: "to_agg1"})
	q.Store(IncomingMessage{CommunicationId: "comm-1", FromClientId: makeClientID(3), Data: []byte("m3"), ToAggregatorName: "to_agg1"})

	got := q.Pull(nil, nil, nil, strPtr("to_agg1"))
	if len(got) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(got))
	}

	// Second read should be empty because queue semantics are consume-on-read.
	if gotAgain := q.Pull(nil, nil, nil, strPtr("to_agg1")); len(gotAgain) != 0 {
		t.Fatalf("expected 0 messages on second read, got %d", len(gotAgain))
	}
}

func TestMessageQueue_SetIndex_NoDuplicateIDsWhenCompositeKeysCollapse(t *testing.T) {
	q := NewMessageStore()

	// Empty comm/client do not create composite indexes.
	q.Store(IncomingMessage{CommunicationId: "", FromClientId: shared.ClientID{}, Data: []byte("m1"), ToAggregatorName: "to_agg1"})

	if len(q.idx) != 1 {
		t.Fatalf("expected only to-aggregator index after collapsed composites, got %d", len(q.idx))
	}

	got := q.Pull(nil, nil, nil, strPtr("to_agg1"))
	if len(got) != 1 {
		t.Fatalf("expected 1 message from full-store read, got %d", len(got))
	}
	if !bytes.Equal(got[0].Data.([]byte), []byte("m1")) {
		t.Fatalf("expected m1, got %s", got[0].Data)
	}
}

func strPtr(v string) *string {
	return &v
}

func TestMessageQueue_PullByAggregatorNameIndex(t *testing.T) {
	q := NewMessageStore()

	clientID1 := makeClientID(1)
	clientID2 := makeClientID(2)
	clientID3 := makeClientID(3)

	q.Store(IncomingMessage{CommunicationId: "comm-1", FromClientId: clientID1, Data: []byte("m1"), ToAggregatorName: "to_agg1"})
	q.Store(IncomingMessage{CommunicationId: "comm-2", FromClientId: clientID2, Data: []byte("m2"), ToAggregatorName: "to_agg2"})
	q.Store(IncomingMessage{CommunicationId: "comm-3", FromClientId: clientID3, Data: []byte("m3"), ToAggregatorName: "to_agg1"})

	got := q.Pull(nil, nil, nil, strPtr("to_agg1"))
	if len(got) != 2 {
		t.Fatalf("expected 2 messages for to_agg1, got %d", len(got))
	}

	payloads := make([]string, 0, len(got))
	for _, msg := range got {
		payloads = append(payloads, string(msg.Data.([]byte)))
		if msg.ToAggregatorName != "to_agg1" {
			t.Fatalf("expected only to_agg1 messages, got %q", msg.ToAggregatorName)
		}
	}
	sort.Strings(payloads)
	if payloads[0] != "m1" || payloads[1] != "m3" {
		t.Fatalf("expected payloads [m1 m3], got %v", payloads)
	}

	if again := q.Pull(nil, nil, nil, strPtr("to_agg1")); len(again) != 0 {
		t.Fatalf("expected 0 messages for to_agg1 after cleanup, got %d", len(again))
	}
	if byComm := q.Pull(strPtr("comm-1"), nil, nil, nil); len(byComm) != 0 {
		t.Fatalf("expected 0 messages for consumed comm-1 index, got %d", len(byComm))
	}
	if byClient := q.Pull(nil, []shared.ClientID{clientID3}, nil, nil); len(byClient) != 0 {
		t.Fatalf("expected 0 messages for consumed client index, got %d", len(byClient))
	}

	remaining := q.Pull(nil, nil, nil, strPtr("to_agg2"))
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining message for to_agg2, got %d", len(remaining))
	}
	if !bytes.Equal(remaining[0].Data.([]byte), []byte("m2")) {
		t.Fatalf("expected m2, got %s", remaining[0].Data)
	}
}

func TestMessageQueue_PullByMultipleClientIDs(t *testing.T) {
	q := NewMessageStore()

	clientID1 := makeClientID(1)
	clientID2 := makeClientID(2)
	clientID3 := makeClientID(3)

	q.Store(IncomingMessage{CommunicationId: "comm-1", FromClientId: clientID1, Data: []byte("m1")})
	q.Store(IncomingMessage{CommunicationId: "comm-2", FromClientId: clientID2, Data: []byte("m2")})
	q.Store(IncomingMessage{CommunicationId: "comm-3", FromClientId: clientID3, Data: []byte("m3")})

	got := q.Pull(nil, []shared.ClientID{clientID1, clientID3}, nil, nil)
	assertPayloads(t, got, []string{"m1", "m3"})

	if again := q.Pull(nil, []shared.ClientID{clientID1, clientID3}, nil, nil); len(again) != 0 {
		t.Fatalf("expected 0 messages for consumed client indexes, got %d", len(again))
	}

	remaining := q.Pull(nil, nil, nil, nil)
	assertPayloads(t, remaining, []string{"m2"})
}

func TestMessageQueue_PullByCommunicationAndMultipleClientIDs(t *testing.T) {
	q := NewMessageStore()

	clientID1 := makeClientID(1)
	clientID2 := makeClientID(2)
	clientID3 := makeClientID(3)

	q.Store(IncomingMessage{CommunicationId: "comm-1", FromClientId: clientID1, Data: []byte("m1")})
	q.Store(IncomingMessage{CommunicationId: "comm-1", FromClientId: clientID2, Data: []byte("m2")})
	q.Store(IncomingMessage{CommunicationId: "comm-2", FromClientId: clientID3, Data: []byte("m3")})

	got := q.Pull(strPtr("comm-1"), []shared.ClientID{clientID1, clientID2, clientID3}, nil, nil)
	assertPayloads(t, got, []string{"m1", "m2"})

	if byComm := q.Pull(strPtr("comm-1"), nil, nil, nil); len(byComm) != 0 {
		t.Fatalf("expected 0 messages for consumed comm-1 index, got %d", len(byComm))
	}

	remaining := q.Pull(nil, []shared.ClientID{clientID3}, nil, nil)
	assertPayloads(t, remaining, []string{"m3"})
}

func TestMessageQueue_PullByAggregatorAndMultipleClientIDs(t *testing.T) {
	q := NewMessageStore()

	clientID1 := makeClientID(1)
	clientID2 := makeClientID(2)
	clientID3 := makeClientID(3)

	q.Store(IncomingMessage{CommunicationId: "comm-1", FromClientId: clientID1, Data: []byte("m1"), ToAggregatorName: "to_agg1"})
	q.Store(IncomingMessage{CommunicationId: "comm-2", FromClientId: clientID2, Data: []byte("m2"), ToAggregatorName: "to_agg2"})
	q.Store(IncomingMessage{CommunicationId: "comm-3", FromClientId: clientID3, Data: []byte("m3"), ToAggregatorName: "to_agg1"})

	got := q.Pull(nil, []shared.ClientID{clientID1, clientID2, clientID3}, nil, strPtr("to_agg1"))
	assertPayloads(t, got, []string{"m1", "m3"})

	remaining := q.Pull(nil, nil, nil, nil)
	assertPayloads(t, remaining, []string{"m2"})
}

func TestMessageQueue_NewIncomingMessageSetsAndPreservesTimestamp(t *testing.T) {
	q := NewMessageStore()

	before := time.Now()
	msg := NewIncomingMessage(
		enums.DefaultSerializationModeByte,
		enums.DefaultCompressionModeByte,
		&shared.DataPacketHeader{
			MemoString:       "comm-1",
			FromClientID:     makeClientID(1),
			ToAggregatorName: "to_agg1",
		},
		[]byte("m1"),
	)
	after := time.Now()

	if msg.TimestampArrived.IsZero() {
		t.Fatal("expected timestamp to be set")
	}
	if msg.TimestampArrived.Before(before) || msg.TimestampArrived.After(after) {
		t.Fatalf("expected timestamp between %v and %v, got %v", before, after, msg.TimestampArrived)
	}

	q.Store(msg)
	got := q.Pull(strPtr("comm-1"), []shared.ClientID{makeClientID(1)}, nil, strPtr("to_agg1"))
	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	if !got[0].TimestampArrived.Equal(msg.TimestampArrived) {
		t.Fatalf("expected timestamp %v, got %v", msg.TimestampArrived, got[0].TimestampArrived)
	}
}

func assertPayloads(t *testing.T, got []IncomingMessage, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("expected %d messages, got %d", len(want), len(got))
	}

	gotPayloads := make([]string, 0, len(got))
	for _, msg := range got {
		gotPayloads = append(gotPayloads, string(msg.Data.([]byte)))
	}
	sort.Strings(gotPayloads)
	sort.Strings(want)
	for i := range want {
		if gotPayloads[i] != want[i] {
			t.Fatalf("expected payloads %v, got %v", want, gotPayloads)
		}
	}
}

func TestOrderByArrivalTime_AscendingOldestFirst(t *testing.T) {
	q := NewMessageStore()
	now := time.Now()

	// Store in reverse order to confirm sort isn't just insertion order.
	q.Store(IncomingMessage{CommunicationId: "c3", FromClientId: makeClientID(3), Data: "newest", TimestampArrived: now.Add(2 * time.Millisecond)})
	q.Store(IncomingMessage{CommunicationId: "c1", FromClientId: makeClientID(1), Data: "oldest", TimestampArrived: now})
	q.Store(IncomingMessage{CommunicationId: "c2", FromClientId: makeClientID(2), Data: "middle", TimestampArrived: now.Add(1 * time.Millisecond)})

	got := q.Pull(nil, nil, nil, nil)
	if len(got) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(got))
	}
	for i, want := range []string{"oldest", "middle", "newest"} {
		if got[i].Data.(string) != want {
			t.Errorf("position %d: got %q, want %q", i, got[i].Data.(string), want)
		}
	}
}

func TestOrderByAutoCommIDCounter_NumericNotLexicographic(t *testing.T) {
	q := NewMessageStore()
	prefix := "AUTOMATIC_COMM_ID_"

	// Lexicographic order would be 1, 10, 9 — numeric order must be 1, 9, 10.
	for _, n := range []string{"10", "1", "9"} {
		q.Store(IncomingMessage{
			CommunicationId:  prefix + n,
			FromClientId:     makeClientID(0),
			Data:             n,
			TimestampArrived: time.Now(),
		})
	}

	fromAgg := ""
	got := q.PullNewestAuto(&fromAgg, nil)
	if got == nil {
		t.Fatal("expected a message, got nil")
	}
	// PullNewestAuto discards all but the highest counter (10).
	if got.Data.(string) != "10" {
		t.Errorf("expected newest counter 10, got %q", got.Data.(string))
	}
	// Store should be empty: all auto messages were consumed.
	if q.Count() != 0 {
		t.Errorf("expected store to be empty after PullNewestAuto, got %d remaining", q.Count())
	}
}

func TestPullGroupedByCommID_ReturnsOnlyCompleteGroups(t *testing.T) {
	q := NewMessageStore()
	prefix := "AUTOMATIC_COMM_ID_"

	// Three messages for counter 1, two for counter 2.
	for i := 0; i < 3; i++ {
		q.Store(IncomingMessage{
			CommunicationId:  prefix + "1",
			FromClientId:     makeClientID(byte(i + 1)),
			Data:             "round1",
			TimestampArrived: time.Now(),
		})
	}
	for i := 0; i < 2; i++ {
		q.Store(IncomingMessage{
			CommunicationId:  prefix + "2",
			FromClientId:     makeClientID(byte(i + 4)),
			Data:             "round2",
			TimestampArrived: time.Now(),
		})
	}

	autoReq := "#AUTOMATIC"
	// With minPackages=3: only counter-1 group qualifies.
	result := q.PullGroupedByCommID(3, &autoReq, nil, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result))
	}
	if msgs, ok := result[prefix+"1"]; !ok {
		t.Error("expected group for AUTOMATIC_COMM_ID_1")
	} else if len(msgs) != 3 {
		t.Errorf("expected 3 messages in group, got %d", len(msgs))
	}

	// Counter-2 group (2 messages) should still be in the store.
	if q.Count() != 2 {
		t.Errorf("expected 2 messages remaining in store, got %d", q.Count())
	}
}
