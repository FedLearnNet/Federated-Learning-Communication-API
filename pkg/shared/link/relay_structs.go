package link

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fc_controller/pkg/shared/enums"
	util "fc_controller/pkg/shared/util"
	"net"
	"sync"
	"sync/atomic"
)

// SPECIAL BYTES
const MAGIC_BYTE_1 = byte('F')
const MAGIC_BYTE_2 = byte('C')

const CMD_STOP = 255

const (
	CMD_SEND_PLAIN_DATA  = 1 + iota
	CMD_SEND_P2P_DATA    = 1 + iota
	CMD_SEND_SMPC_DATA   = 1 + iota
	CMD_SEND_SMPC_AGG    = 1 + iota
	CMD_INIT_CONNECTION  = 1 + iota
	CMD_SEND_PUBLIC_KEY  = 1 + iota
	CMD_SEND_PUBLIC_KEYS = 1 + iota
)

const (
	SMPC_OP_ADD = 1 + iota
)

type ClientID [8]byte

// ZERO_CLIENT_ID is a special destination client ID.
// If coordinator uses this as destination, the package is broadcast to all clients.
// If a client uses this as destination, the message is sent to the coordinator.
var ZERO_CLIENT_ID = ClientID{}

func (cid ClientID) MarshalJSON() ([]byte, error) {
	s := cid.ToString()
	return json.Marshal(s)
}

func (cid *ClientID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := ClientIDFromString(s)
	if err != nil {
		return err
	}
	*cid = parsed
	return nil
}

// Helper String encoding/decoding
func (cid ClientID) ToString() string {
	return hex.EncodeToString(cid[:])
}
func ClientIDFromString(s string) (ClientID, error) {
	cid := ClientID{}
	bts, err := hex.DecodeString(s)
	if err != nil {
		return ClientID{}, err
	}
	if len(bts) != len(cid) {
		return ClientID{}, errors.New("invalid client ID string")
	}
	copy(cid[:], bts)
	return cid, nil
}

type RelayChannel [32]byte

func NewChannel() RelayChannel {
	rc := RelayChannel{}
	_, _ = rand.Read(rc[:])
	return rc
}

func ChannelFromString(s string) (RelayChannel, error) {
	rc := RelayChannel{}
	src := []byte(s)
	if hex.DecodedLen(len(src)) != len(rc) {
		return RelayChannel{}, errors.New("invalid channel string")
	}
	_, _ = hex.Decode(rc[:], src)
	return rc, nil
}

func (rc RelayChannel) ToString() string {
	dst := make([]byte, hex.EncodedLen(len(rc[:])))
	hex.Encode(dst, rc[:])
	return string(dst)
}

func (rc RelayChannel) MarshalJSON() ([]byte, error) {
	s := rc.ToString()
	return json.Marshal(s)
}

func (rc *RelayChannel) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := ChannelFromString(s)
	if err != nil {
		return err
	}
	*rc = parsed
	return nil
}

type SetupBody struct {
	ID          ClientID         `json:"id"`
	Coordinator ClientID         `json:"coordinator"`
	Clients     []ClientID       `json:"clients"`
	Keys        []util.PublicKey `json:"keys"`
}

// TCPIOInterface is the minimal byte-level interface needed by the protocol header codecs.
// Implement this on custom guarded readers/writers to use shared header logic directly.
type TCPIOInterface interface {
	ReadByte() (byte, error)
	ReadBytes([]byte) error
	WriteByte(byte) error
	WriteBytes([]byte) error
}

// ErrStopped is returned by TCPIO read and write methods when the connection was
// closed via Stop() or a registered stop channel. Callers can use errors.Is to
// distinguish a deliberate shutdown from an unexpected connection error.
var ErrStopped = errors.New("tcpio: stopped")

// ErrConnectionClosed is returned by TCPIO read and write methods when the connection
// was closed unexpectedly (not via Stop()). This indicates an error condition rather
// than a deliberate shutdown. Callers can use errors.Is to distinguish this from ErrStopped.
var ErrConnectionClosed = errors.New("tcpio: connection closed")

// TCPIO provides methods for reading and writing over TCP connections.
// Used especially for header reading/writing
// State values (managed externally):
// - StateInit: initial state, connection established
// - StateRunning: actively processing operations
// - StateFinished: deliberately stopped via Stop() or stop channel
// - StateError: connection error or closed unexpectedly
type TCPIO struct {
	conn      net.Conn
	readLock  sync.Mutex // Guards Read operations
	writeLock sync.Mutex // Guards Write operations
	// state is a pointer to external state (typically FLRunHandle.state)
	// This allows the connection and run to share the same lifecycle.
	// When TCPIO encounters an error, the shared state reflects it immediately.
	state *atomic.Uint32
}

var _ TCPIOInterface = (*TCPIO)(nil)

// NewTCPIO creates a new TCPIO for the given connection.
// The state parameter (typically FLRunHandle.state or FlRunMeta.State) is optional.
// If provided, the connection and run share the same lifecycle. If nil, the connection
// operates independently (useful for pre-authentication handshakes where the run is not
// yet determined). State can be nil during handshake and added later if needed.
func NewTCPIO(conn net.Conn, state *atomic.Uint32) *TCPIO {
	// state is optional - nil is acceptable for connections before run assignment
	return &TCPIO{conn: conn, state: state}
}

// Stop marks the connection as intentionally stopped and closes it.
// All blocked and future reads/writes return ErrStopped.
// Idempotent — safe to call multiple times.
func (h *TCPIO) Stop() error {
	// Transition to StateFinished (deliberately stopped) if currently active
	enums.TryMarkFinished(h.state)
	return h.Close()
}

// lockHeaderRead acquires the TCPIO read lock and returns its paired unlock function.
// Callers should always defer the returned function.
func (h *TCPIO) LockHeaderRead() (func(), error) {
	h.readLock.Lock()
	if h.state != nil && h.state.Load() == enums.StateFinished {
		h.readLock.Unlock()
		return nil, ErrStopped
	}
	if h.state != nil && h.state.Load() == enums.StateError {
		h.readLock.Unlock()
		return nil, ErrConnectionClosed
	}
	return h.readLock.Unlock, nil
}

// lockHeaderWrite acquires the TCPIO write lock and returns its paired unlock function.
// Callers should always defer the returned function.
func (h *TCPIO) LockHeaderWrite() (func(), error) {
	h.writeLock.Lock()
	if h.state != nil && h.state.Load() == enums.StateFinished {
		h.writeLock.Unlock()
		return nil, ErrStopped
	}
	if h.state != nil && h.state.Load() == enums.StateError {
		h.writeLock.Unlock()
		return nil, ErrConnectionClosed
	}
	return h.writeLock.Unlock, nil
}

// WithHeaderReadLock executes fn while holding the TCP read lock.
// The lock is always released before returning.
func (h *TCPIO) WithHeaderReadLock(fn func(io TCPIOInterface) error) error {
	unlock, err := h.LockHeaderRead()
	if err != nil {
		return err
	}
	defer unlock()

	return fn(h)
}

// WithHeaderWriteLock executes fn while holding the TCP write lock.
// The lock is always released before returning.
func (h *TCPIO) WithHeaderWriteLock(fn func(io TCPIOInterface) error) error {
	unlock, err := h.LockHeaderWrite()
	if err != nil {
		return err
	}
	defer unlock()

	return fn(h)
}

func (h *TCPIO) Close() error {
	// Try to transition from any active state to StateError
	if h.state != nil {
		enums.TryMarkError(h.state)
	}

	// Closing the connection immediately unblocks any goroutines
	// stuck in ReadByte() or WriteBytes() with a "use of closed network connection" error.
	return h.conn.Close()
}

// NewClientID creates a random ClientID that is guaranteed to not be ZERO_CLIENT_ID.
func NewClientID() (ClientID, error) {
	tries := 0
	for {
		if tries > 100 {
			return ClientID{}, errors.New("failed to generate non-zero client ID after 100 attempts")
		}
		cid := ClientID{}
		if _, err := rand.Read(cid[:]); err != nil {
			return ClientID{}, err
		}
		if cid != ZERO_CLIENT_ID {
			// SUCCESS
			return cid, nil
		}
		tries++
	}
}

type FinishedStates struct {
	CoordinatorFinished bool `json:"coordinatorFinished"`
	ClientFinished      int  `json:"clientsFinished"`
}

// PtrStringChanged returns true if one pointer is nil and the other isn't,
// or both are non-nil but point to different values.
func PtrStringChanged(s1 *string, s2 *string) bool {
	return (s1 != nil || s2 != nil) && ((s1 != nil && s2 == nil) || (s1 == nil && s2 != nil) || *s1 != *s2)
}

// PtrFloat64Changed returns true if one pointer is nil and the other isn't,
// or both are non-nil but point to different values.
func PtrFloat64Changed(f1 *float64, f2 *float64) bool {
	return (f1 != nil || f2 != nil) && ((f1 != nil && f2 == nil) || (f1 == nil && f2 != nil) || *f1 != *f2)
}

func (h *TCPIO) ReadCmd() (byte, error) {
	return h.readByte()
}

// ReadByte reads one byte from the underlying connection.
func (h *TCPIO) ReadByte() (byte, error) {
	return h.readByte()
}

// ReadBytes reads exactly len(bs) bytes from the underlying connection.
func (h *TCPIO) ReadBytes(bs []byte) error {
	return h.readBytes(bs)
}

// WriteByte writes one byte to the underlying connection.
func (h *TCPIO) WriteByte(b byte) error {
	return h.writeByte(b)
}

// WriteBytes writes all bytes to the underlying connection.
func (h *TCPIO) WriteBytes(bs []byte) error {
	return h.writeBytes(bs)
}

// readByte reads a single byte from the connection.
func (h *TCPIO) readByte() (byte, error) {
	bs := [1]byte{}
	err := h.readBytes(bs[:])
	if err != nil {
		return 0, err
	}
	return bs[0], nil
}

// readBytes reads exactly len(bs) bytes from the connection.
func (h *TCPIO) readBytes(bs []byte) error {
	for len(bs) > 0 {
		n, err := h.conn.Read(bs)
		if err != nil {
			if h.state != nil && h.state.Load() == enums.StateFinished {
				return ErrStopped
			}
			return err
		}
		if n == 0 {
			return errors.New("connection closed unexpectedly")
		}
		bs = bs[n:]
	}
	return nil
}

// writeByte writes a single byte to the connection.
func (h *TCPIO) writeByte(b byte) error {
	bs := [1]byte{b}
	return h.writeBytes(bs[:])
}

// writeBytes writes all bytes to the connection.
func (h *TCPIO) writeBytes(b []byte) error {
	n, err := h.conn.Write(b)
	if err != nil {
		if h.state != nil && h.state.Load() == enums.StateFinished {
			return ErrStopped
		}
		return err
	}
	if n != len(b) {
		return errors.New("failed to write all bytes to connection")
	}
	return nil
}
