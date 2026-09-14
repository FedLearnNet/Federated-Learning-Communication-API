'''
Helper script expecting a started controller and relay server instance.
Includes multiple federated learning tests, for instance
- plain data send/receive (JSON and CBOR), peer to peer and gather and broadcast
- differential privacy (DP) and secure multi-party computation (SMPC) tests
- tests the automatic comm ID generation and sorting of messages by the controller
See the comments above each test case for more information
'''
import requests
import json
import io
import cbor2
import time

RELAY_ADDRESS_HTTP = "http://127.0.0.1:9140"
CONTROLLER_ADDRESS_ORCH_HTTP = "http://127.0.0.1:8002"
CONTROLLER_ADDRESS_FLCOMM_HTTP = "http://127.0.0.1:8003"
RUN_ID = "test_run_id"

PLAIN_COMM_ID = "plain_comm_id"
SMPC_COMM_ID = "smpc_comm_id"
DP_COMM_ID = "dp_comm_id"
P2P_COMM_ID = "p2p_test_round_1"

AGGREGATOR_PLAIN_JSON = "aggregator_plain_json"
AGGREGATOR_PLAIN_CBOR = "aggregator_plain_cbor"
AGGREGATOR_SMPC = "aggregator_smpc"
AGGREGATOR_DP = "aggregator_dp"
AGGREGATOR_AUTO = "aggregator_auto_test"
AGGREGATOR_DUMMY = "aggregator_dummy"  # for testing that another aggregator doesnt interfere
AGGREGATOR_P2P = "aggregator_p2p_test"

# Matches models.AutoCommIDPrefix in the controller.
AUTO_COMM_PREFIX = "AUTOMATIC_COMM_ID_"

PLAIN_DATA_SEND = 10
# SMPC: each client sends [1.0, 2.0, 3.0], coordinator should receive sum [3.0, 6.0, 9.0]
SMPC_DATA_SEND = [1.0, 2.0, 3.0]
SMPC_EXPONENT = 8
# DP: each client sends 10.0 with Laplace noise; result should be close to 10.0
DP_DATA_SEND = 10.0
AGG_BROADCAST_DATA = 77
P2P_DATA_1 = 11
P2P_DATA_2 = 22

NUM_CLIENTS = 3


# =============================================
# Infrastructure helpers
# =============================================

def wait_for_services(address):
    max_retries = 60
    for i in range(max_retries):
        time.sleep(0.5)
        try:
            response = requests.get(address + "/healthz", timeout=5)
            if response.status_code == 200:
                return
        except Exception:
            pass
    raise TimeoutError(f"Service at {address} is not healthy after {max_retries} retries")


def create_fl_run_on_relay(num_clients=NUM_CLIENTS):
    """Create a new FL run on the relay. Returns relay response dict."""
    response = requests.post(
        RELAY_ADDRESS_HTTP + "/create-fl-run",
        json={"maxNumClients": num_clients, "appVersion": "v2"},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def start_learning_on_controller(client_id, client_key, run_info):
    """Register one participant (client or coordinator) with the controller."""
    response = requests.post(
        CONTROLLER_ADDRESS_ORCH_HTTP + "/start-learning",
        json={
            "channel": run_info["channel"],
            "clientId": client_id,
            "clientKey": client_key,
            "relayKey": run_info["relayKey"],
            "runId": RUN_ID,
            "coordinatorId": run_info["coordinatorId"],
            "maxNumClients": run_info.get("maxNumClients", NUM_CLIENTS),
            "orderClientIds": run_info["clientIds"],
            "appKey": client_id,
            "appVersion": "v2",
        },
        timeout=20,
    )
    label = "coordinator" if client_id == run_info["coordinatorId"] else f"client {client_id}"
    if response.status_code == 200:
        print(f"  start-learning OK for {label}")
    else:
        print(f"  start-learning FAIL for {label}: {response.status_code} - {response.text}")


def setup_fl_run(num_clients=NUM_CLIENTS):
    """Create a FL run on the relay, register all participants, and wait for connections.

    Returns a run_info dict augmented with coordinator_id and channel for convenience.
    """
    run_info = create_fl_run_on_relay(num_clients)
    for client_id in run_info["clientIds"]:
        client_key = run_info["clientId2ClientKey"][client_id]
        start_learning_on_controller(client_id, client_key, run_info)
    coordinator_key = run_info["coordinatorKey"]
    start_learning_on_controller(run_info["coordinatorId"], coordinator_key, run_info)
    # Allow TCP connections and key exchange to settle.
    time.sleep(0.5)
    return run_info


def request_receive_setup(client_id, channel):
    """POST /receive-setup for client_id. Returns setup dict or None on failure."""
    try:
        response = requests.post(
            CONTROLLER_ADDRESS_FLCOMM_HTTP + "/receive-setup",
            json={"appKey": client_id, "channel": channel},
            timeout=15,
        )
        if response.status_code == 200:
            return response.json()
        print(f"  receive-setup failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"  receive-setup error: {e}")
    return None


# =============================================
# Send helpers
# =============================================

def _encode_payload(data, serialization_format):
    if serialization_format == "json":
        return json.dumps(data).encode("utf-8")
    if serialization_format == "cbor":
        return cbor2.dumps(data)
    raise ValueError(f"Unsupported serialization format: {serialization_format}")


def send_to_aggregator(client_id, channel, data, to_aggregator, comm_id=None,
                       serialization_format="json", smpc=None, dp=None):
    """POST /send-data-to-aggregator (client → aggregator).

    comm_id=None / "" / "#AUTOMATIC"  → automatic comm ID (MemoSize=0)
    Any other string                  → manual comm ID; duplicate re-use returns 409 + kills run.

    Returns HTTP status code.
    """
    metadata = {
        "appKey": client_id,
        "channel": channel,
        "serializationUsed": serialization_format,
        "toAggregator": to_aggregator,
        "communicationId": comm_id,
        "smpc": smpc,
        "dp": dp,
    }
    data_bytes = _encode_payload(data, serialization_format)
    files = {
        "metadata": (None, json.dumps(metadata), "application/json"),
        "data": ("payload.bin", io.BytesIO(data_bytes), "application/octet-stream"),
    }
    response = requests.post(
        CONTROLLER_ADDRESS_FLCOMM_HTTP + "/send-data-to-aggregator",
        files=files,
        timeout=10,
    )
    if response.status_code == 200:
        print(f"  send-to-aggregator OK  client={client_id} comm_id={comm_id!r} to {to_aggregator}")
    else:
        print(f"  send-to-aggregator {response.status_code}  client={client_id} comm_id={comm_id!r} to {to_aggregator}: {response.text}")
    return response.status_code


def send_to_clients(sender_id, channel, data, comm_id, from_aggregator=None, to=None,
                    serialization_format="json", dp=None):
    """POST /send-data-to-clients (aggregator/coordinator → clients, or P2P).

    comm_id is REQUIRED and must be a non-empty, non-"#AUTOMATIC" string.
    When responding to a round, the coordinator echoes the auto comm ID it received
    (e.g. "AUTOMATIC_COMM_ID_1") so receiving clients can identify the round.

    Returns HTTP status code.
    """
    metadata = {
        "appKey": sender_id,
        "channel": channel,
        "serializationUsed": serialization_format,
        "communicationId": comm_id,
        "fromAggregator": from_aggregator,
        "to": to or [],
        "dp": dp,
    }
    data_bytes = _encode_payload(data, serialization_format)
    files = {
        "metadata": (None, json.dumps(metadata), "application/json"),
        "data": ("payload.bin", io.BytesIO(data_bytes), "application/octet-stream"),
    }
    response = requests.post(
        CONTROLLER_ADDRESS_FLCOMM_HTTP + "/send-data-to-clients",
        files=files,
        timeout=10,
    )
    if response.status_code == 200:
        print(f"  send-to-clients OK  sender={sender_id} comm_id={comm_id!r}")
    else:
        print(f"  send-to-clients {response.status_code}  sender={sender_id} comm_id={comm_id!r}: {response.text}")
    return response.status_code


# =============================================
# Receive helpers
# =============================================

def receive_from_aggregator(client_id, channel, from_aggregator, comm_id=None,
                             requested_format="json"):
    """POST /receive-data-from-aggregator (client ← aggregator).

    comm_id=None / "" / "#AUTOMATIC"  → return the SINGLE NEWEST auto-comm-ID message;
                                         all older auto messages for this aggregator are discarded.
    Any other string                  → return the message with that specific comm ID.

    Returns message dict on success, None if no message (204) or on error.
    """
    payload = {
        "appKey": client_id,
        "channel": channel,
        "serializationFormat": requested_format,
        "fromAggregator": from_aggregator,
        "communicationId": comm_id,
    }
    try:
        response = requests.post(
            CONTROLLER_ADDRESS_FLCOMM_HTTP + "/receive-data-from-aggregator",
            json=payload,
            timeout=15,
        )
        if response.status_code == 200:
            if requested_format == "json":
                return response.json()
            return cbor2.loads(response.content)
        if response.status_code == 204:
            return None
        print(f"  receive-from-aggregator {response.status_code}: {response.text}")
        return None
    except Exception as e:
        print(f"  receive-from-aggregator error: {e}")
        return None


def receive_from_clients(client_id, channel, min_packages, to_aggregator=None, comm_id=None,
                          from_client_ids=None, requested_format="json"):
    """POST /receive-data-from-clients (aggregator/coordinator ← clients, or P2P).

    Returns only comm ID groups with ≥ min_packages messages.
    comm_id=None / ""    → all comm IDs (manual and auto)
    comm_id="#AUTOMATIC" → auto-comm-ID messages only
    Any other string     → only that specific comm ID group

    Returns map[commID][]messages (possibly empty if no complete groups yet), or None on error.
    """
    payload = {
        "appKey": client_id,
        "channel": channel,
        "serializationFormat": requested_format,
        "toAggregator": to_aggregator,
        "communicationId": comm_id,
        "fromClientIds": from_client_ids or [],
        "minPackages": min_packages,
    }
    try:
        response = requests.post(
            CONTROLLER_ADDRESS_FLCOMM_HTTP + "/receive-data-from-clients",
            json=payload,
            timeout=15,
        )
        if response.status_code == 200:
            if requested_format == "json":
                return response.json()
            return cbor2.loads(response.content)
        print(f"  receive-from-clients {response.status_code}: {response.text}")
        return None
    except Exception as e:
        print(f"  receive-from-clients error: {e}")
        return None


def receive_from_clients_status(client_id, channel, min_packages):
    """Call /receive-data-from-clients and return only the HTTP status code (for state checks)."""
    payload = {
        "appKey": client_id,
        "channel": channel,
        "serializationFormat": "json",
        "minPackages": min_packages,
    }
    try:
        response = requests.post(
            CONTROLLER_ADDRESS_FLCOMM_HTTP + "/receive-data-from-clients",
            json=payload,
            timeout=10,
        )
        return response.status_code
    except Exception:
        return None


# =============================================
# Poll helpers
# =============================================

def poll_from_aggregator(client_id, channel, from_aggregator, timeout_s=20, comm_id=None,
                          requested_format="json"):
    """Poll /receive-data-from-aggregator until a message arrives or timeout.

    Returns the message dict, or None on timeout.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msg = receive_from_aggregator(client_id, channel, from_aggregator, comm_id, requested_format)
        if msg is not None:
            return msg
        time.sleep(0.5)
    return None


def poll_from_clients(client_id, channel, min_packages, to_aggregator=None, comm_id=None,
                       timeout_s=20, requested_format="json"):
    """Poll /receive-data-from-clients until at least one complete group is returned.

    Returns the grouped map (possibly empty if timed out with no complete groups).
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        grouped = receive_from_clients(
            client_id, channel, min_packages,
            to_aggregator=to_aggregator, comm_id=comm_id,
            requested_format=requested_format,
        )
        if grouped:  # non-empty map: at least one complete group
            return grouped
        time.sleep(0.5)
    return {}


def flat_messages(grouped):
    """Flatten a map[commID][]messages to a plain list."""
    msgs = []
    for group in grouped.values():
        msgs.extend(group)
    return msgs


# =============================================
# Prerequisites: ensure relay server and controller are running and healthy
# =============================================
print("Checking relay server health...")
wait_for_services(RELAY_ADDRESS_HTTP)
print("Relay server is healthy")
print("Checking controller health...")
wait_for_services(CONTROLLER_ADDRESS_ORCH_HTTP)
print("Controller is healthy")

# =============================================
# Setup: create FL run and register all participants
# =============================================
print("\n--- Setting up FL run ---")
run_info = setup_fl_run()
coordinator_id = run_info["coordinatorId"]
coordinator_key = run_info["coordinatorKey"]
client_ids = run_info["clientIds"]
client_id_to_client_key = run_info["clientId2ClientKey"]
channel = run_info["channel"]
relay_key = run_info["relayKey"]

# =============================================
# REQUEST SETUP INFO FROM CONTROLLER
# =============================================
print("\n--- Requesting setup info from controller ---")
for client_id in client_ids:
    setup_info = request_receive_setup(client_id, channel)
    if setup_info:
        print(f"  Client {client_id} received setup info: {setup_info}")
    else:
        print(f"  FAIL: Client {client_id} did not receive setup info")
        exit(1)

# =============================================
# PLAIN DATA TEST — JSON with auto comm ID
# =============================================
print("\n--- Plain data test JSON ---")
plain_comm_id_json = PLAIN_COMM_ID + "_json"
for client_id in client_ids:
    send_to_aggregator(client_id, channel, PLAIN_DATA_SEND, AGGREGATOR_PLAIN_JSON, comm_id=plain_comm_id_json)

grouped = poll_from_clients(coordinator_id, channel, NUM_CLIENTS,
                             to_aggregator=AGGREGATOR_PLAIN_JSON)
if len(grouped) != 1:
    print(f"  FAIL: expected messages to be grouped under exactly 1 comm ID, got {len(grouped)} groups: {list(grouped.keys())}")
    exit(1)
communication_id = list(grouped.keys())[0]
if communication_id != plain_comm_id_json:
    print(f"  FAIL: expected auto comm ID prefix in {communication_id!r}")
    exit(1)
msgs_received = flat_messages(grouped)
print(f"Coordinator received {len(msgs_received)}/{NUM_CLIENTS} plain messages")
if len(msgs_received) != NUM_CLIENTS:
    print(f"  FAIL: expected {NUM_CLIENTS} messages, got {len(msgs_received)}")
    exit(1)
for idx, msg in enumerate(msgs_received):
    if msg["data"] != PLAIN_DATA_SEND:
        print(f"  FAIL message {idx+1}: got {msg['data']}, expected {PLAIN_DATA_SEND}")
        exit(1)
print("  All values correct")

# Coordinator broadcasts back to all clients.
send_to_clients(coordinator_id, channel, PLAIN_DATA_SEND, communication_id,
                from_aggregator=AGGREGATOR_PLAIN_JSON)
for client_id in client_ids:
    msg = poll_from_aggregator(client_id, channel, AGGREGATOR_PLAIN_JSON, comm_id=plain_comm_id_json)
    if msg is None:
        print(f"  FAIL: no message received for client {client_id}")
        exit(1)
    if msg["meta"]["communicationId"] != plain_comm_id_json:
        print(f"  FAIL: expected communicationId {plain_comm_id_json!r}, got {msg['meta']['communicationId']!r}")
        exit(1)
    if msg["data"] != PLAIN_DATA_SEND:
        print(f"  FAIL client {client_id}: got {msg['data']}, expected {PLAIN_DATA_SEND}")
        exit(1)
print("  Broadcast received correctly by all clients")
print("Plain data test JSON PASSED")

# =============================================
# PLAIN DATA TEST — CBOR with manual comm id
# =============================================
print("\n--- Plain data test CBOR ---")
plain_comm_id_cbor = PLAIN_COMM_ID + "_cbor"
for client_id in client_ids:
    send_to_aggregator(client_id, channel, PLAIN_DATA_SEND, AGGREGATOR_PLAIN_CBOR,
                       comm_id=plain_comm_id_cbor, serialization_format="cbor")

grouped = poll_from_clients(coordinator_id, channel, NUM_CLIENTS,
                             to_aggregator=AGGREGATOR_PLAIN_CBOR, comm_id=plain_comm_id_cbor,
                             requested_format="cbor")
communication_id = list(grouped.keys())[0] if grouped else None
if communication_id != plain_comm_id_cbor:
    print(f"  FAIL: expected communicationId {plain_comm_id_cbor!r}, got {communication_id!r}")
    exit(1)
msgs_received = flat_messages(grouped)
print(f"Coordinator received {len(msgs_received)}/{NUM_CLIENTS} plain messages")
if len(msgs_received) != NUM_CLIENTS:
    print(f"  FAIL: expected {NUM_CLIENTS} messages, got {len(msgs_received)}")
    exit(1)
for idx, msg in enumerate(msgs_received):
    if msg["data"] != PLAIN_DATA_SEND:
        print(f"  FAIL message {idx+1}: got {msg['data']}, expected {PLAIN_DATA_SEND}")
        exit(1)
print("  All values correct")

send_to_clients(coordinator_id, channel, PLAIN_DATA_SEND, plain_comm_id_cbor,
                from_aggregator=AGGREGATOR_PLAIN_CBOR, serialization_format="cbor")
for client_id in client_ids:
    msg = poll_from_aggregator(client_id, channel, AGGREGATOR_PLAIN_CBOR,
                                comm_id=plain_comm_id_cbor, requested_format="cbor")
    if msg is None:
        print(f"  FAIL: no message received for client {client_id}")
        exit(1)
    if msg["data"] != PLAIN_DATA_SEND:
        print(f"  FAIL client {client_id}: got {msg['data']}, expected {PLAIN_DATA_SEND}")
        exit(1)
print("  Broadcast received correctly by all clients")
print("Plain data test CBOR PASSED")

# =============================================
# DP TEST
# =============================================
print("\n--- DP test ---")
# epsilon=10, clippingVal=10 → no clipping (|10|≤10), sensitivity=2*10=20, scale=20/10=2
dp_params = {
    "noisetype": "laplace",
    "epsilon": 10.0,
    "delta": 0,
    "sensitivity": None,
    "clippingVal": 10.0,
    "enabled": True,
}
for client_id in client_ids:
    send_to_aggregator(client_id, channel, DP_DATA_SEND, AGGREGATOR_DP,
                       comm_id=DP_COMM_ID, dp=dp_params)

grouped = poll_from_clients(coordinator_id, channel, NUM_CLIENTS,
                             to_aggregator=AGGREGATOR_DP, comm_id=DP_COMM_ID)
dp_msgs = flat_messages(grouped)
print(f"Coordinator received {len(dp_msgs)}/{NUM_CLIENTS} DP messages")
if len(dp_msgs) != NUM_CLIENTS:
    print(f"  FAIL: expected {NUM_CLIENTS} messages, got {len(dp_msgs)}")
    exit(1)
for idx, msg in enumerate(dp_msgs):
    val = msg["data"]
    print(f"  DP message {idx+1}: {val:.4f} (sent {DP_DATA_SEND}, noise applied)")
    # Laplace(scale=2): P(|noise|>15) < 0.0001%
    if not (DP_DATA_SEND - 15 <= val <= DP_DATA_SEND + 15):
        print(f"  FAIL: DP value {val} too far from expected {DP_DATA_SEND}")
        exit(1)
    if val == DP_DATA_SEND:
        print(f"  FAIL: DP value {val} has no noise, which is statistically unlikely")
        exit(1)
print("DP test PASSED (noised values within expected range)")

# =============================================
# SMPC TEST
# =============================================
print("\n--- SMPC test ---")
smpc_params = {
    "operation": "add",
    "exponent": SMPC_EXPONENT,
    "numShards": NUM_CLIENTS,
    "enabled": True,
}
for client_id in client_ids:
    send_to_aggregator(client_id, channel, SMPC_DATA_SEND, AGGREGATOR_SMPC,
                       comm_id=SMPC_COMM_ID, smpc=smpc_params)

# SMPC pipeline has multiple relay hops; allow extra time.
grouped = poll_from_clients(coordinator_id, channel, 1,
                             to_aggregator=AGGREGATOR_SMPC, comm_id=SMPC_COMM_ID,
                             timeout_s=10)
smpc_msgs = flat_messages(grouped)
print(f"Coordinator received {len(smpc_msgs)} SMPC aggregation result(s)")
if not smpc_msgs:
    print("FAIL: no SMPC result received at coordinator")
    exit(1)
for idx, msg in enumerate(smpc_msgs):
    result = msg["data"]
    expected = [v * NUM_CLIENTS for v in SMPC_DATA_SEND] if isinstance(result, list) else SMPC_DATA_SEND * NUM_CLIENTS
    print(f"  SMPC result {idx+1}: {result}  (expected ≈ {expected})")
    tolerance = 0.1
    if isinstance(result, list):
        for r, e in zip(result, expected):
            if abs(r - e) > tolerance:
                print(f"  FAIL: {r} differs from expected {e} by more than {tolerance}")
                exit(1)
    else:
        if abs(result - expected) > tolerance:
            print(f"  FAIL: {result} differs from expected {expected} by more than {tolerance}")
            exit(1)
print("SMPC test PASSED")

# =============================================
# AUTO COMM ID + SORT TEST
# All previous tests used manual comm IDs, so auto counters are still at 0.
# 1. client_fast sends one message with another AGGREGATOR_DUMMY to ensure this
#   doesnt poison the auto counter for AGGREGATOR_AUTO (counters are per-aggregator).
# 2. client_fast sends one message to AGGREGATOR_AUTO with no comm_id (round 1)
# 3. Aggregator already retrieves the message (simulating skipping of slower client 2) (round 1)
# Check whether the auto comm ID is correctly generated (AUTO_COMM_ID_1)
# and that the dummy aggregator message didnt interfere
# 4. Aggregator broadcasts back to clients (round 1 response)
# 5. client_slow sends one message to AGGREGATOR_AUTO with no comm_id (old round 1, simulates client 2 lagging behind)
# 6. client_fast receives the round 1 response with the round 1 auto comm ID (AUTO_COMM_ID_1)
# 7. client_fast sends one message to AGGREGATOR_AUTO with no comm_id (round 2)
# 8. Aggregator retrieves the two messages.
# Check whether the round 1 message from client_slow is correctly discarded as outdated
# 9. Aggregator broadcasts back to clients (round 2 response)
# 10. Both clients retrive now
# 11. client_fast sends one message to AGGREGATOR_AUTO with no comm_id (round 3)
# 12. client_slow sends one message to AGGREGATOR_AUTO with no comm_id (round 3), skipping round 2 sending
# 13. aggregator retrieves the two messages of round 3.
# Check whether client_slow sends with round 3 effectively skipping round 2 sending
# =============================================
print("\n--- Auto comm ID + sort test ---")
client_fast = client_ids[0]
client_slow = client_ids[1]
round1_id = AUTO_COMM_PREFIX + "1"
round2_id = AUTO_COMM_PREFIX + "2"
round3_id = AUTO_COMM_PREFIX + "3"

# Step 1: dummy send — must not affect AGGREGATOR_AUTO counter (counters are per-aggregator)
send_to_aggregator(client_fast, channel, 999, AGGREGATOR_DUMMY, comm_id=None)
# Step 2: client_fast sends round 1 to AGGREGATOR_AUTO
send_to_aggregator(client_fast, channel, 1, AGGREGATOR_AUTO, comm_id=None)

# Step 3: coordinator retrieves round 1 early (client_slow hasn't sent yet, min_packages=1)
grouped = poll_from_clients(coordinator_id, channel, 1,
                             to_aggregator=AGGREGATOR_AUTO, timeout_s=10)
if len(grouped) != 1:
    print(f"  FAIL: expected exactly 1 group ({round1_id!r}), got {list(grouped.keys())}")
    exit(1)
if round1_id not in grouped:
    print(f"  FAIL: expected group {round1_id!r} not found, got {list(grouped.keys())}")
    exit(1)
if len(grouped[round1_id]) != 1:
    print(f"  FAIL: expected 1 message in {round1_id!r}, got {len(grouped[round1_id])}")
    exit(1)
print(f"  Step 3 OK: coordinator retrieved {round1_id!r} with 1 message")

# Verify dummy counter is independent: AGGREGATOR_DUMMY also has its own AUTOMATIC_COMM_ID_1
grouped_dummy = poll_from_clients(coordinator_id, channel, 1,
                                   to_aggregator=AGGREGATOR_DUMMY, timeout_s=10)
if not grouped_dummy or round1_id not in grouped_dummy:
    print(f"  FAIL: expected dummy to have {round1_id!r}, got {list(grouped_dummy.keys()) if grouped_dummy else 'empty'}")
    exit(1)
print(f"  Step 3 OK: AGGREGATOR_DUMMY has its own {round1_id!r} (per-aggregator counters work)")

# Step 4: coordinator broadcasts round 1 response
send_to_clients(coordinator_id, channel, 100, round1_id, from_aggregator=AGGREGATOR_AUTO)

# Step 5: client_slow sends its round 1 late (simulates lagging behind)
send_to_aggregator(client_slow, channel, 1, AGGREGATOR_AUTO, comm_id=None)

# Step 6: client_fast retrieves the round 1 response with explicit comm_id
msg = poll_from_aggregator(client_fast, channel, AGGREGATOR_AUTO, comm_id=round1_id, timeout_s=10)
if msg is None:
    print(f"  FAIL: client_fast did not receive round 1 response ({round1_id!r})")
    exit(1)
if msg["meta"]["communicationId"] != round1_id:
    print(f"  FAIL: expected {round1_id!r}, got {msg['meta']['communicationId']!r}")
    exit(1)
print(f"  Step 6 OK: client_fast retrieved round 1 response ({round1_id!r})")

# Step 7: client_fast sends round 2
send_to_aggregator(client_fast, channel, 2, AGGREGATOR_AUTO, comm_id=None)

# Step 8: coordinator retrieves — expects only client_fast's round 2
# round 1 from client_slow should be discarded as stale
deadline = time.time() + 10
grouped = {}
while time.time() < deadline:
    grouped = receive_from_clients(coordinator_id, channel, 1, to_aggregator=AGGREGATOR_AUTO) or {}
    if round2_id in grouped:
        break
    time.sleep(0.5)
if len(grouped) != 1:
    print(f"  FAIL: expected exactly 1 group ({round2_id!r}), got {len(grouped)} groups: {sorted(grouped.keys())}")
    exit(1)
if set(grouped.keys()) != {round2_id}:
    print(f"  FAIL: expected group {{{round2_id!r}}}, got {sorted(grouped.keys())}")
    exit(1)

print(f"  Step 8 OK: coordinator sees only client_fast's {round2_id!r}")

# Step 9: coordinator broadcasts round 2 response (ignores client_slow's stale round 1)
send_to_clients(coordinator_id, channel, 200, round2_id, from_aggregator=AGGREGATOR_AUTO)

# Step 10: both clients retrieve using auto-newest (no comm_id)
msg = poll_from_aggregator(client_fast, channel, AGGREGATOR_AUTO, comm_id=None, timeout_s=10)
if msg is None or msg["meta"]["communicationId"] != round2_id:
    print(f"  FAIL: client_1 expected {round2_id!r}, got {(msg['meta']['communicationId'] if msg else None)!r}")
    exit(1)
print(f"  Step 10 OK: client_1 retrieved {round2_id!r}")

# client_2 has both AUTOMATIC_COMM_ID_1 (step 4 broadcast) and AUTOMATIC_COMM_ID_2 (step 9 broadcast).
# Auto-newest must return AUTOMATIC_COMM_ID_2 and discard the stale AUTOMATIC_COMM_ID_1.
# Receiving AUTOMATIC_COMM_ID_2 also triggers UpdateAutoCommIdCounterFromReceived: 2 > 1 → counter advanced to 2.
msg = poll_from_aggregator(client_slow, channel, AGGREGATOR_AUTO, comm_id=None, timeout_s=10)
if msg is None or msg["meta"]["communicationId"] != round2_id:
    print(f"  FAIL: client_2 expected {round2_id!r} (stale {round1_id!r} discarded), got {(msg['meta']['communicationId'] if msg else None)!r}")
    exit(1)
print(f"  Step 10 OK: client_2 retrieved {round2_id!r} (stale {round1_id!r} discarded, counter advanced 1→2)")

# Steps 11+12: both clients send round 3.
# client_1 counter: 2 (from step 7 send) → 3 → AUTOMATIC_COMM_ID_3
# client_2 counter: advanced to 2 (by receiving AUTOMATIC_COMM_ID_2 above) → 3 → AUTOMATIC_COMM_ID_3
send_to_aggregator(client_fast, channel, 3, AGGREGATOR_AUTO, comm_id=None)
send_to_aggregator(client_slow, channel, 3, AGGREGATOR_AUTO, comm_id=None)

# Step 13: coordinator retrieves round 3 — must be exactly 1 group (AUTOMATIC_COMM_ID_3) with 2 messages.
# If client_2's counter was NOT advanced, it would send AUTOMATIC_COMM_ID_2 instead, giving 2 groups.
grouped = poll_from_clients(coordinator_id, channel, 2,
                             to_aggregator=AGGREGATOR_AUTO, timeout_s=10)
if len(grouped) != 1 or round3_id not in grouped:
    print(f"  FAIL: expected exactly 1 group ({round3_id!r}), got {sorted(grouped.keys())}")
    exit(1)
if len(grouped[round3_id]) != 2:
    print(f"  FAIL: expected 2 messages in {round3_id!r}, got {len(grouped[round3_id])}")
    exit(1)
print(f"  Step 13 OK: coordinator received 2 messages under {round3_id!r}")
print(f"  client_2 correctly skipped round 2 send and jumped to round 3 (counter advanced by received message)")
print("Auto comm ID + sort test PASSED")

# =============================================
# MIN-PACKAGES TEST
# Each client's auto counter starts at 1 (no previous auto sends from these clients).
# client_1 sends to AGGREGATOR_AUTO (second auto from client_1 after the 2 above → counter 3).
# client_2 sends to AGGREGATOR_AUTO (first auto from client_2 → counter 1).
# These have different counters so they won't form a group together.
# Instead, we test that min_packages=2 with only 1 message returns empty,
# then after a second message arrives with the SAME counter, the group is complete.
#
# To get matching counters: create a fresh run so both clients start at counter 1.
# =============================================
print("\n--- MinPackages test (fresh FL run) ---")
print("  Creating fresh FL run for MinPackages test...")
fresh_run = setup_fl_run()
fresh_coord = fresh_run["coordinatorId"]
fresh_clients = fresh_run["clientIds"]
fresh_channel = fresh_run["channel"]
fresh_client_1 = fresh_clients[0]
fresh_client_2 = fresh_clients[1]

# Both clients have never sent any auto messages → counter starts at 1.
# client_1 sends first; coordinator checks immediately (only 1 message so far).
send_to_aggregator(fresh_client_1, fresh_channel, 10, AGGREGATOR_AUTO, comm_id=None)

# Give the message time to arrive, then check with min_packages=2 → expect empty.
time.sleep(0.3)
grouped = receive_from_clients(fresh_coord, fresh_channel, 2, to_aggregator=AGGREGATOR_AUTO)
if grouped is None:
    print("  FAIL: receive-from-clients returned None (unexpected error)")
    exit(1)
if grouped:
    print(f"  FAIL: expected empty map with only 1 message (min_packages=2), got: {list(grouped.keys())}")
    exit(1)
print("  Correct: coordinator gets empty map when only 1 of 2 required messages present")

# Now client_2 sends (also first auto → AUTOMATIC_COMM_ID_1, same as client_1's).
send_to_aggregator(fresh_client_2, fresh_channel, 20, AGGREGATOR_AUTO, comm_id=None)

# Coordinator polls with min_packages=2; now both messages with the same auto comm ID present.
grouped = poll_from_clients(fresh_coord, fresh_channel, 2,
                             to_aggregator=AGGREGATOR_AUTO, timeout_s=10)
if not grouped:
    print("  FAIL: expected complete group after 2 clients sent, got empty map")
    exit(1)
cids = list(grouped.keys())
if len(cids) != 1 or not cids[0].startswith(AUTO_COMM_PREFIX):
    print(f"  FAIL: expected exactly 1 auto comm ID group, got: {cids}")
    exit(1)
group_msgs = grouped[cids[0]]
if len(group_msgs) != 2:
    print(f"  FAIL: expected 2 messages in group, got {len(group_msgs)}")
    exit(1)
print(f"  Complete group {cids[0]!r} with {len(group_msgs)} messages received")
print("MinPackages test PASSED")

# =============================================
# SECURITY TEST: duplicate comm ID → run fails → 410 Gone
# A fresh FL run is used so the security test does not affect other runs.
# =============================================
print("\n--- Security test: duplicate comm ID → run fails → 410 ---")
print("  Creating fresh FL run for security test...")
sec_run = setup_fl_run()
sec_coord = sec_run["coordinatorId"]
sec_clients = sec_run["clientIds"]
sec_channel = sec_run["channel"]
sec_client_1 = sec_clients[0]

DUP_COMM_ID = "dup_security_test"

# First send with DUP_COMM_ID → should succeed (200).
code = send_to_aggregator(sec_client_1, sec_channel, 1, AGGREGATOR_AUTO, comm_id=DUP_COMM_ID)
if code != 200:
    print(f"  FAIL: first send returned {code}, expected 200")
    exit(1)
print("  First send OK (200)")

# Second send with the same DUP_COMM_ID → duplicate detected → 409 + run fails asynchronously.
code = send_to_aggregator(sec_client_1, sec_channel, 2, AGGREGATOR_AUTO, comm_id=DUP_COMM_ID)
if code != 409:
    print(f"  FAIL: duplicate send returned {code}, expected 409")
    exit(1)
print("  Duplicate send rejected (409)")

# Wait for async MarkError goroutine to transition the run to StateError.
deadline = time.time() + 2.0
gone_confirmed = False
while time.time() < deadline:
    status = receive_from_clients_status(sec_client_1, sec_channel, 1)
    if status == 410:
        gone_confirmed = True
        break
    time.sleep(0.1)

if not gone_confirmed:
    print("  FAIL: expected 410 Gone after duplicate comm ID, but run did not transition to error state")
    exit(1)
print("  Run correctly returned 410 Gone after duplicate comm ID")
print("Security test PASSED")


# =============================================
# PEER-TO-PEER TEST
# One normal aggregation round (auto comm ID) followed by two clients sending P2P
# messages to a third client. The second client sends a message twice with the same comm ID
# This is allowed in peer to peer and should not cause an error
# Verifies that the third client can separately retrieve
# the auto broadcast (via receive-from-aggregator with no comm ID) and the three P2P
# messages (via receive-from-clients with the explicit P2P comm ID).
# =============================================
print("\n--- Peer-to-peer test ---")
print("  Creating fresh FL run for peer-to-peer test...")
p2p_run = setup_fl_run()
p2p_coord = p2p_run["coordinatorId"]
p2p_clients = p2p_run["clientIds"]
p2p_channel = p2p_run["channel"]

p2p_client_1 = p2p_clients[0]
p2p_client_2 = p2p_clients[1]
p2p_client_3 = p2p_clients[2]

# Step 1: Normal aggregation round with auto comm ID.
print("  Step 1: all clients send to aggregator (auto comm ID)...")
for p2p_client in p2p_clients:
    send_to_aggregator(p2p_client, p2p_channel, AGG_BROADCAST_DATA, AGGREGATOR_P2P, comm_id=None)

grouped = poll_from_clients(p2p_coord, p2p_channel, NUM_CLIENTS,
                             to_aggregator=AGGREGATOR_P2P, timeout_s=15)
if not grouped:
    print("  FAIL: coordinator did not receive aggregation messages")
    exit(1)
auto_comm_id = list(grouped.keys())[0]
if not auto_comm_id.startswith(AUTO_COMM_PREFIX):
    print(f"  FAIL: expected auto comm ID, got {auto_comm_id!r}")
    exit(1)
print(f"  Coordinator received {len(flat_messages(grouped))} messages under {auto_comm_id!r}")

# Coordinator broadcasts the result back to all clients, echoing the auto comm ID.
code = send_to_clients(p2p_coord, p2p_channel, AGG_BROADCAST_DATA, auto_comm_id,
                       from_aggregator=AGGREGATOR_P2P)
if code != 200:
    print(f"  FAIL: coordinator broadcast returned {code}")
    exit(1)
print(f"  Coordinator broadcast {auto_comm_id!r} back to all clients")

# Step 2: client_1 and client_2 send P2P messages to client_3.
print(f"  Step 2: client_1 and client_2 send P2P to client_3 (comm_id={P2P_COMM_ID!r})...")
code = send_to_clients(p2p_client_1, p2p_channel, P2P_DATA_1, P2P_COMM_ID, to=[p2p_client_3])
if code != 200:
    print(f"  FAIL: client_1 P2P send returned {code}")
    exit(1)
code = send_to_clients(p2p_client_2, p2p_channel, P2P_DATA_2, P2P_COMM_ID, to=[p2p_client_3])
if code != 200:
    print(f"  FAIL: client_2 P2P send returned {code}")
    exit(1)
code = send_to_clients(p2p_client_2, p2p_channel, P2P_DATA_2, P2P_COMM_ID, to=[p2p_client_3])
if code != 200:
    print(f"  FAIL: client_2 duplicate P2P send returned {code}")
    exit(1)
print("  P2P messages sent")

# Step 3a: client_3 receives with no comm ID → only the auto broadcast is returned;
# the P2P messages must remain untouched in the store.
print("  Step 3a: client_3 polls for auto broadcast (no comm ID)...")
msg = poll_from_aggregator(p2p_client_3, p2p_channel, AGGREGATOR_P2P, comm_id=None, timeout_s=15)
if msg is None:
    print("  FAIL: client_3 did not receive auto broadcast")
    exit(1)
if not msg["meta"]["communicationId"].startswith(AUTO_COMM_PREFIX):
    print(f"  FAIL: expected auto comm ID, got {msg['meta']['communicationId']!r}")
    exit(1)
if msg["data"] != AGG_BROADCAST_DATA:
    print(f"  FAIL: expected broadcast data {AGG_BROADCAST_DATA}, got {msg['data']}")
    exit(1)
print(f"  client_3 received auto broadcast {msg['meta']['communicationId']!r} (data={msg['data']})")

# Step 3b: client_3 retrieves the three P2P messages by explicit comm ID.
print(f"  Step 3b: client_3 polls for P2P messages (comm_id={P2P_COMM_ID!r}, min_packages=3)...")
p2p_grouped = poll_from_clients(p2p_client_3, p2p_channel, 3,
                                 comm_id=P2P_COMM_ID, timeout_s=15)
if not p2p_grouped:
    print(f"  FAIL: client_3 did not receive 3 P2P messages under {P2P_COMM_ID!r}")
    exit(1)
if P2P_COMM_ID not in p2p_grouped:
    print(f"  FAIL: expected key {P2P_COMM_ID!r} in result, got {list(p2p_grouped.keys())}")
    exit(1)
if len(p2p_grouped) != 1:
    print(f"  FAIL: expected only 1 comm ID group, got {len(p2p_grouped)}: {list(p2p_grouped.keys())}")
    exit(1)
p2p_msgs = p2p_grouped[P2P_COMM_ID]
if len(p2p_msgs) != 3:
    print(f"  FAIL: expected 3 P2P messages, got {len(p2p_msgs)}")
    exit(1)
expected_mapping = {
    p2p_client_1: P2P_DATA_1,
    p2p_client_2: P2P_DATA_2,
}
for msg in p2p_msgs:
    sender = msg.get("meta", {}).get("fromClientId", None)
    if not sender:
        print(f"  FAIL: message missing fromClientId in meta: {msg}")
        exit(1)
    if sender not in expected_mapping:
        print(f"  FAIL: unexpected sender {sender!r} in P2P messages")
        exit(1)
    expected_data = expected_mapping[sender]
    if msg["data"] != expected_data:
        print(f"  FAIL: for sender {sender!r}, expected data {expected_data}, got {msg['data']}")
        exit(1)
print(f"  client_3 received {len(p2p_msgs)} P2P messages with data {[msg['data'] for msg in p2p_msgs]}")
print("Peer-to-peer test PASSED")


print("\n=== All tests completed successfully ===")
