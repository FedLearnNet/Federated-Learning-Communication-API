package enums

import (
	"fmt"
	"sync/atomic"
)

type AppVersionEnum string

const (
	AppVersionV1 AppVersionEnum = "v1"
	AppVersionV2 AppVersionEnum = "v2"
)

func (v AppVersionEnum) Validate() error {
	switch v {
	case AppVersionV1, AppVersionV2:
		return nil
	default:
		return fmt.Errorf("unsupported app version: %s", v)
	}
}

func StringToAppVersionEnum(s string) (AppVersionEnum, error) {
	switch s {
	case "v1":
		return AppVersionV1, nil
	case "v2":
		return AppVersionV2, nil
	default:
		return "", fmt.Errorf("unsupported app version: %s", s)
	}
}

// State constants used for standardized state management across components.
// Used to track lifecycle states in FL runs, connections, and other stateful operations.
const (
	StateInit uint32 = iota
	StateRunning
	StateError
	StateFinished
)

// IsActive returns true if the given state indicates an active/operational component.
// A state is considered active if it is not in a terminal error or finished state.
// This provides a standardized way to check if operations can proceed on the component.
func IsActive(state uint32) bool {
	return state != StateError && state != StateFinished
}

// TryMarkFinished attempts to transition a state to StateFinished.
// Returns true if the transition succeeded (was in StateInit or StateRunning).
// Returns false if already in a terminal state (StateFinished or StateError).
// This is idempotent — multiple calls when already finished are safe and return false.
func TryMarkFinished(state *atomic.Uint32) bool {
	if state.CompareAndSwap(StateInit, StateFinished) {
		return true
	}
	if state.CompareAndSwap(StateRunning, StateFinished) {
		return true
	}
	// Already in terminal state (StateFinished or StateError)
	return false
}

// TryMarkError attempts to transition a state to StateError.
// Tries all non-terminal states via a compare-and-swap loop.
// Returns true if the transition succeeded, false if already in a terminal state.
// This is idempotent — multiple calls when already error/finished are safe and return false.
func TryMarkError(state *atomic.Uint32) bool {
	oldState := state.Load()
	for oldState != StateError && oldState != StateFinished {
		if state.CompareAndSwap(oldState, StateError) {
			return true
		}
		oldState = state.Load()
	}
	return false
}
