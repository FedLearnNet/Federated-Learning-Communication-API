package link

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

// --- Mocks ---

// MockTCPIO satisfies TCPIOInterface and simulates network behavior using a buffer.
type MockTCPIO struct {
	Buffer    *bytes.Buffer
	Closed    bool
	readLock  sync.Mutex
	writeLock sync.Mutex
}

func NewMockTCPIO() *MockTCPIO {
	return &MockTCPIO{
		Buffer: new(bytes.Buffer),
	}
}

// --- TCPIOInterface Implementation ---

func (m *MockTCPIO) ReadByte() (byte, error) {
	if m.Buffer.Len() == 0 {
		return 0, errors.New("EOF")
	}
	return m.Buffer.ReadByte()
}

func (m *MockTCPIO) ReadBytes(bs []byte) error {
	n, err := m.Buffer.Read(bs)
	if n != len(bs) {
		return errors.New("unexpected EOF: not enough bytes in mock buffer")
	}
	return err
}

func (m *MockTCPIO) WriteByte(b byte) error {
	return m.Buffer.WriteByte(b)
}

func (m *MockTCPIO) WriteBytes(bs []byte) error {
	_, err := m.Buffer.Write(bs)
	return err
}

func (m *MockTCPIO) LockHeaderRead() (func(), error) {
	m.readLock.Lock()
	return m.readLock.Unlock, nil
}

func (m *MockTCPIO) LockHeaderWrite() (func(), error) {
	m.writeLock.Lock()
	return m.writeLock.Unlock, nil
}

// --- Protocol Helper (Bridge) ---
// Since your real code has Read/Write methods on the TCPIO struct,
// you need to ensure the methods you're calling in tests are either
// defined on the interface or available to the mock.

func (m *MockTCPIO) readByte() (byte, error) { return m.ReadByte() }

// --- Tests ---

func TestNewClientID(t *testing.T) {
	cid, err := NewClientID()
	if err != nil {
		t.Fatalf("Failed to generate ClientID: %v", err)
	}
	if cid == ZERO_CLIENT_ID {
		t.Error("NewClientID generated a ZERO_CLIENT_ID")
	}
}

func TestClientID_JSON(t *testing.T) {
	cid, _ := NewClientID()
	data, err := json.Marshal(cid)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var cid2 ClientID
	err = json.Unmarshal(data, &cid2)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if cid != cid2 {
		t.Errorf("Expected %v, got %v", cid, cid2)
	}
}

func TestRelayChannel_ToString(t *testing.T) {
	ch := NewChannel()
	s := ch.ToString()
	ch2, err := ChannelFromString(s)
	if err != nil {
		t.Fatalf("Failed to convert channel string: %v", err)
	}

	if ch != ch2 {
		t.Errorf("Channel conversion failed: expected %v, got %v", ch, ch2)
	}
}

func TestPtrStringChanged(t *testing.T) {
	s1 := "hello"
	s2 := "world"
	s3 := "hello"

	tests := []struct {
		a, b     *string
		expected bool
	}{
		{nil, nil, false},
		{&s1, nil, true},
		{nil, &s1, true},
		{&s1, &s2, true},
		{&s1, &s3, false},
	}

	for _, tt := range tests {
		if res := PtrStringChanged(tt.a, tt.b); res != tt.expected {
			t.Errorf("PtrStringChanged(%v, %v) = %v; want %v", tt.a, tt.b, res, tt.expected)
		}
	}
}

func TestTCPIO_Lifecycle(t *testing.T) {
	// Use a pipe to simulate a real net.Conn
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	// Create a shared state (normally owned by FLRunHandle)
	var state atomic.Uint32
	io := NewTCPIO(c1, &state)

	// Test Locking
	unlock, err := io.LockHeaderRead()
	if err != nil {
		t.Fatalf("Failed to lock: %v", err)
	}
	unlock()

	// Test Write/Read
	go func() {
		_ = io.WriteBytes([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	}()

	buf := make([]byte, 4)
	_, err = c2.Read(buf) // Read from other side of pipe
	if err != nil || !bytes.Equal(buf, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Errorf("Data mismatch: %v", buf)
	}

	// Test Close
	io.Close()
	if _, err := io.LockHeaderRead(); err == nil {
		t.Error("Expected error locking a closed TCPIO")
	}
}
