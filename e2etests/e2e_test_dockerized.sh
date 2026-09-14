#!/usr/bin/env bash
set -e
# Runs end-to-end tests:
# Builds/Pulls relay and controller docker images and uses these to start
# one relay server
# one controller
# Then runs the python test script which simulates
# multiple clients all using the same controller
# see the python script for details on the test cases

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

### CONFIG
# Images to use in testing
# If you add something also build/pull it and add it to the testing below
LOCAL_RELAY_IMAGE="fc_relay_e2etest"
LOCAL_CONTROLLER_IMAGE="fc_controller_e2etest"
STAGING_RELAY_IMAGE="gitlab.cosy.bio:5050/cosybio/federated-learning/federated_db/feature-cloud-controller/controller-relay:staging"
STAGING_CONTROLLER_IMAGE="gitlab.cosy.bio:5050/cosybio/federated-learning/federated_db/feature-cloud-controller/controller:staging"

# Container names and log prefixes
RELAY_CONTAINER="fc_relay_e2etest"
CONTROLLER_CONTAINER="fc_controller_e2etest"
RELAY_LOG_PREFIX="$SCRIPT_DIR/tmp_relay"
CONTROLLER_LOG_PREFIX="$SCRIPT_DIR/tmp_controller"


### BUILD / PULL IMAGES
# Build/pull images required
echo "Pulling relay image..."
docker pull "$STAGING_RELAY_IMAGE"
echo "Pulling controller image..."
docker pull "$STAGING_CONTROLLER_IMAGE"

echo "Building relay image..."
docker build -t "$LOCAL_RELAY_IMAGE" -f Dockerfile.relay .
echo "Building controller image..."
docker build -t "$LOCAL_CONTROLLER_IMAGE" -f Dockerfile.dev .

### Main test function
# Actual tests are further below
cleanup() {
    # cleanup any potentially running containers
    echo "Stopping relay and controller containers..."
    docker kill "$RELAY_CONTAINER" 2>/dev/null || true
    docker kill "$CONTROLLER_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

run_test_case() {
    local relay_image="$1"
    local controller_image="$2"
    local test_number="$3"
    local network_mode="$4"
    local tls_mode="${5:-self-signed}"
    local relay_log="${RELAY_LOG_PREFIX}.test${test_number}.log"
    local controller_log="${CONTROLLER_LOG_PREFIX}.test${test_number}.log"
    local description="${network_mode} networking, tls=${tls_mode}"

    echo ""
    echo "=== TEST ${test_number}: ${description} ==="
    if [[ "$network_mode" == "bridge" ]]; then
        local relay_flags="--network bridge -p 9140:9140 -p 9141:9141"
        local controller_flags="--network bridge -p 8002:8002 -p 8003:8003 --add-host=host.docker.internal:host-gateway"
    else
        local relay_flags="--network host"
        local controller_flags="--network host"
    fi

    docker run --rm --name "$RELAY_CONTAINER" \
        $relay_flags \
        -e RELAY_MODE=dev \
        -e RELAY_TLS_MODE="$tls_mode" \
        -e RELAY_DOMAIN=localhost,host.docker.internal \
        --health-cmd "bash -ec 'exec 3<>/dev/tcp/127.0.0.1/9140; printf \"GET /healthz HTTP/1.1\\r\\nHost: localhost:8000\\r\\nConnection: close\\r\\n\\r\\n\" >&3; grep -q \"200 OK\" <&3'" \
        --health-interval 5s \
        --health-timeout 3s \
        --health-retries 20 \
        "$relay_image" >"$relay_log" 2>&1 &
    RELAY_PID=$!
    echo "Relay container started (PID $RELAY_PID) → $relay_log"


    docker run --rm --name "$CONTROLLER_CONTAINER" \
        $controller_flags \
        -e RELAY_MODE=dev \
        -e RELAY_TLS_MODE="$tls_mode" \
        -e FL_RUN_MANAGER_PORT=8002 \
        -e APPCOMMV2_PORT=8003 \
        -e RELAY_ADDRESS_TCP=host.docker.internal:9141 \
        --health-cmd "bash -ec 'exec 3<>/dev/tcp/127.0.0.1/8002; printf \"GET /healthz HTTP/1.1\\r\\nHost: localhost:8002\\r\\nConnection: close\\r\\n\\r\\n\" >&3; grep -q \"200 OK\" <&3'" \
        --health-interval 5s \
        --health-timeout 3s \
        --health-retries 20 \
        "$controller_image" >"$controller_log" 2>&1 &
    CONTROLLER_PID=$!
    echo "Controller container started (PID $CONTROLLER_PID) → $controller_log"
    echo ""
    echo "=== Running Test ${test_number} (${description}) ==="
    output=$(python3 "$SCRIPT_DIR/e2e_test_helper.py")
    status=$?

    if [ $status -ne 0 ]; then
        echo "=== Tests failed ==="
        echo "$output"
        exit $status
    else
        echo "=== Tests successful ==="
    fi
    echo "=== Python test finished: TEST ${test_number} (${description}) ==="
}

# Dev images
run_test_case "$LOCAL_RELAY_IMAGE" "$LOCAL_CONTROLLER_IMAGE" "1" "bridge" "self-signed"
run_test_case "$LOCAL_RELAY_IMAGE" "$LOCAL_CONTROLLER_IMAGE" "2" "bridge" "off"
run_test_case "$LOCAL_RELAY_IMAGE" "$LOCAL_CONTROLLER_IMAGE" "3" "host" "self-signed"

# Staging images
run_test_case "$RELAY_IMAGE" "$CONTROLLER_IMAGE" "4" "bridge" "self-signed"
run_test_case "$RELAY_IMAGE" "$CONTROLLER_IMAGE" "5" "bridge" "off"
run_test_case "$RELAY_IMAGE" "$CONTROLLER_IMAGE" "6" "host" "self-signed"
