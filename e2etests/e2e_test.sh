#!/usr/bin/env bash
# Runs end-to-end tests:
# Compiles the relay server and controller and uses these to start
# one relay server
# one controller
# Then runs the python test script which simulates
# multiple clients all using the same controller
# see the python script for details on the test cases
set -e

# Run from project root regardless of where the script is invoked from
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

RELAY_BIN="/tmp/fc_relay_test"
CONTROLLER_BIN="/tmp/fc_controller_test"
RELAY_LOG="$SCRIPT_DIR/tmp_relay.log"
CONTROLLER_LOG="$SCRIPT_DIR/tmp_controller.log"

# Kill any leftover binaries from previous runs
pkill -f "$RELAY_BIN" 2>/dev/null || true
pkill -f "$CONTROLLER_BIN" 2>/dev/null || true
sleep 0.3

# Build fresh binaries
echo "Building relay..."
go build -o "$RELAY_BIN" ./cmd/relay
echo "Building controller..."
go build -o "$CONTROLLER_BIN" ./cmd/controller

cleanup() {
    status=$?
    echo "Stopping relay and controller..."
    kill "$RELAY_PID" "$CONTROLLER_PID" 2>/dev/null || true
    wait "$RELAY_PID" "$CONTROLLER_PID" 2>/dev/null || true

    if [[ $status -ne 0 ]]; then
        echo ""
        echo "=== Relay log (error exit) ==="
        cat "$RELAY_LOG" 2>/dev/null || echo "(relay log unavailable)"
        echo ""
        echo "=== Controller log (error exit) ==="
        cat "$CONTROLLER_LOG" 2>/dev/null || echo "(controller log unavailable)"
        echo ""
    fi
}
trap cleanup EXIT

# -------------------------------------------------------------
# 1. Run without TLS
# -------------------------------------------------------------
echo "Starting Relay without TLS..."
RELAY_MODE=dev RELAY_TLS_MODE=off RELAY_DOMAIN=127.0.0.1 "$RELAY_BIN" >"$RELAY_LOG" 2>&1 &
RELAY_PID=$!
echo "Relay started (PID $RELAY_PID) → $RELAY_LOG"

echo "Starting Controller without TLS..."
RELAY_MODE=dev RELAY_TLS_MODE=off RELAY_ADDRESS_TCP=127.0.0.1:9141 "$CONTROLLER_BIN" >"$CONTROLLER_LOG" 2>&1 &
CONTROLLER_PID=$!
echo "Controller started (PID $CONTROLLER_PID) → $CONTROLLER_LOG"

echo ""
echo "=== Running Python test without TLS ==="
output=$(python3 "$SCRIPT_DIR/e2e_test_helper.py")
status=$?

if [ $status -ne 0 ]; then
    echo "Tests failed:"
    echo "$output"
    exit $status
else
    echo "Tests successful"
fi

# Stop services
kill "$RELAY_PID" "$CONTROLLER_PID" 2>/dev/null || true
wait "$RELAY_PID" "$CONTROLLER_PID" 2>/dev/null || true
sleep 0.5

# -------------------------------------------------------------
# 2. Run with self-signed TLS
# -------------------------------------------------------------
echo "Starting Relay with self-signed TLS..."
RELAY_MODE=dev RELAY_TLS_MODE=self-signed RELAY_DOMAIN=127.0.0.1 "$RELAY_BIN" >"$RELAY_LOG" 2>&1 &
RELAY_PID=$!
echo "Relay started (PID $RELAY_PID) → $RELAY_LOG"

echo "Starting Controller with self-signed TLS..."
RELAY_MODE=dev RELAY_TLS_MODE=self-signed RELAY_ADDRESS_TCP=127.0.0.1:9141 "$CONTROLLER_BIN" >"$CONTROLLER_LOG" 2>&1 &
CONTROLLER_PID=$!
echo "Controller started (PID $CONTROLLER_PID) → $CONTROLLER_LOG"

echo ""
echo "=== Running Python test with self-signed TLS ==="
output=$(python3 "$SCRIPT_DIR/e2e_test_helper.py")
status=$?

if [ $status -ne 0 ]; then
    echo "Tests failed:"
    echo "$output"
    exit $status
else
    echo "Tests successful"
fi
