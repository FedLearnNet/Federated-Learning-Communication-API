# FL-Net Federated Learning Communication

Two Go services to build federated learning networks and run federated learning:

- **Controller:** one per federated learning node; relays data between the local app and the network
- **Relay server:** one per network; all controllers connect to it

## Documentation

- [FL-Net documentation](https://federated-learning.net/documentation/)

## Features

- All communication between controllers is end-to-end encrypted, except the broadcasting of data from the aggregator
- Both SMPC and DP are supported
- Clients may drop out without the federated learning process stopping if the app is configured for this
  - SMPC is an exception, as it creates shards that are sent from one client to ALL other clients. All shards are required to reconstruct the data.
- Controllers authenticate towards the relay server and the relay server authenticates towards the controllers
  - mTLS: each federated learning run has its own CA on the relay server, which signs one client certificate per
    controller. The private key never leaves the controller. The controllers cert signing is handled via the secure channel between local and global learning APIs.
  - Additionally, both sides exchange run-specific keys on the application layer

## Quick start

Requirements: Go (version in `go.mod`), Docker for the images, Python 3 for the end-to-end tests.

```bash
go build -o relay ./cmd/relay
go build -o controller ./cmd/controller

./relay --mode dev --tls-mode self-signed --domain localhost
./controller --address-tcp localhost:9141 --mode dev --tls-mode self-signed
```

Or start a relay and a controller with the published staging images: `docker compose up`.

## Testing

```bash
gofmt -l .                                   # must print nothing
go test ./...                                # unit tests
pip install -r e2etests/requirements.txt
bash e2etests/e2e_test.sh                    # end-to-end tests (plain, DP and SMPC message flows)
```



### Relay server

Deploy via `Dockerfile.relay` or build natively with `go build -o relay ./cmd/relay`.
The relay server starts an HTTP server on port `9140` and a TCP server on port `9141`.

All configuration can be set via CLI flags, environment variables or a `config.yml` file.
See `cmd/relay/main.go` for the complete list of flags. Key options:

- **Mode**: `--mode` or env `RELAY_MODE` (default: `prod`) — set to `dev` for development
- **TLS Mode**: `--tls-mode` or env `RELAY_TLS_MODE` (default: `on`) — options: `off`, `self-signed`, `on`
  - `prod` mode enforces `on` only (validates certificate chains)
  - `dev` mode allows `self-signed` for testing without valid certificates
- **Domain**: `--domain` or env `RELAY_DOMAIN` (required for TLS) — e.g. `localhost`, `example.com`
- **HTTP Port**: `--http-port` or env `RELAY_HTTP_PORT` (default: `9140`)
- **TCP Port**: `--tcp-port` or env `RELAY_TCP_PORT` (default: `9141`)
- **Log Levels**: `--logLevelStdOut` and `--logLevelFile` (default: `info` and `debug`)

```bash
docker run -e RELAY_MODE=dev -e RELAY_TLS_MODE=self-signed -e RELAY_DOMAIN=localhost \
  ghcr.io/fedlearnnet/federated-learning-communication-api/controller-relay:staging
```

> Note: when connecting through Docker host networking or a bridge host alias such as `docker.host.internal`, the
> self-signed certificate must be issued for that exact hostname, e.g. `--domain docker.host.internal`, and the
> controller must connect to `docker.host.internal:9141`.

### Controller

Deploy via `Dockerfile`, `Dockerfile.staging` or `Dockerfile.dev` (they only differ in the embedded config file) or
build natively with `go build -o controller ./cmd/controller`.

Configuration is read from CLI flags, environment variables and a `config.yml` file, in this order of priority.
Otherwise the defaults in `cmd/controller/main.go` apply. Key options:

- **Relay Address**: `--address-tcp` or env `RELAY_ADDRESS_TCP` (default: `localhost:9150`) — where to connect to the relay server
- **Relay Mode**: `--mode` or env `RELAY_MODE` (default: `prod`) — must match the relay server (`dev` or `prod`)
- **Relay TLS Mode**: `--tls-mode` or env `RELAY_TLS_MODE` (default: `on`) — must match the relay server (`off`, `self-signed`, `on`)
- **HTTP Port**: `--flrunmanagerport` or env `FL_RUN_MANAGER_PORT` (default: `8080`)
- **AppCommV2 Port**: `--appcommv2port` or env `APPCOMMV2_PORT` (default: `8081`)
- **Log Levels**: `--logLevelStdOut` and `--logLevelFile` (default: `info` and `debug`)
- **Learning API**: `--learning-api-ws` or env `WORKFLOW_LEARNING_API_WS_ADDRESS` (default: `ws://localhost:8080/ws`)

```bash
docker run -e RELAY_ADDRESS_TCP=relay:9141 -e RELAY_MODE=dev -e RELAY_TLS_MODE=self-signed \
  -e FL_RUN_MANAGER_PORT=8080 -e APPCOMMV2_PORT=8081 \
  ghcr.io/fedlearnnet/federated-learning-communication-api/controller:staging
```

The controller connects to:

- a relay server (configured via `--address-tcp`)
- for apps of version 1: a local learning API (websocket), not yet implemented

## Architecture

### Package: Shared

Helper code used by both the controller and the relay server, e.g. read/write helpers for data sent between them or
logging:

- `pkg/shared/logger`: `Logger`
- `pkg/shared/link`: read and write helpers for messages between controller and relay server
- `pkg/shared/models`: common models used by both, e.g. a client ID
- `pkg/shared/util`: common utilities such as crypto helpers

### Package: Relay Server

#### Purpose

The relay server creates a federated learning run, including the relevant authentication keys, lets clients
(controller instances) connect and relays information. It also keeps track of the run's meta information, e.g. the
public keys of clients. It does NOT orchestrate federated learning runs; this is done via the HTTP server it exposes.

#### Components

- `pkg/relayserver/http`: `RelayServiceHTTP`: HTTP server to create and stop federated learning runs and to sign
  the client certificates of a run (`/create-fl-run`, `/sign-fl-run-cert`, `/stop-fl-run`)
- `pkg/relayserver/tcp`: `RelayServiceTCP`: TCP server the controllers connect to for relaying information
- `pkg/relayserver/bridge`: `FLRunStore`: map with the meta information of the ongoing federated learning runs.
  Managed by `RelayServiceHTTP` and used by `RelayServiceTCP`. Each run has an atomic `State` field for lifecycle
  coordination between the HTTP and TCP components.

The entrypoint is `cmd/relay/main.go`.

#### Flow

`main.go` creates the `FLRunStore` and starts both `RelayServiceHTTP` and `RelayServiceTCP`. A created run is added
to the store together with a CA generated for this run, where `RelayServiceTCP` finds it when clients connect.
With TLS enabled, a client is only accepted if it presents a certificate signed by the CA of its run for its own
client ID (requested via `/sign-fl-run-cert`, one key pair per client) and knows its client key. When `RelayServiceHTTP` stops a run, it marks
the run's `State` as finished. The TCP relay goroutines detect this via `IsActive()` and exit cleanly, closing all
connections and cleaning up pending messages.

### Package: Controller

#### Purpose

Each controller represents one federated learning node and controls the flow there. It:

- relays information from a federated learning app into the network formed by multiple controllers and one relay
  server
- applies privacy enhancing techniques such as SMPC and DP as well as end-to-end encryption for peer-to-peer messages

The controller does NOT orchestrate apps; an external service starts and stops federated learning runs.

#### Components

- `pkg/controller`: `FLRunManagerServiceHTTP`: HTTP server on the `flrunmanagerport`, receives orchestration requests
- `pkg/controller`: `FLRunManagerBO`: manages the lifecycle of individual FL runs and owns multiple `flRunOrch` instances:
  - `map[FlRunKey]flRunOrch`:
    - `pkg/controller`: `flRunOrch`:
      - `pkg/controller`: `FLRunHandle`: shared handle containing the communication bridge for a run:
        - `pkg/controller/bridge`: `MessageStore` / `MessageQueue` / `FLRunMeta` (one per run)
      - `pkg/controller/relaycomm`: `RelayClient`
      - `pkg/controller/appcomm`: `AppCommunicatorV1` (one per run, V1 only)
      - `pkg/controller/learningcomm`: `LearningApiCommunicator` (websocket client, one per run, V1 only)
  - `pkg/controller/appcomm`: `AppCommunicatorV2`: singleton HTTP server on the `appcommv2port`, used for multiple
    concurrent runs. Holds a `map[FlRunKey]*FLRunHandle`, each entry pointing to the `FLRunHandle` owned by the
    corresponding `flRunOrch`.
- `pkg/controller/data`: HTTP models, enums, etc.

The entrypoint is `cmd/controller/main.go`.

#### Flow

##### Start of a federated learning run

On startup, the controller starts the `FLRunManagerService` and the `AppCommunicatorV2`, which is shared by all
concurrent runs.

When the `FLRunManagerService` receives a request to start a run, the `FLRunManagerBO`:

- creates an `FLRunHandle` containing
  - a `MessageStore` for incoming messages
  - a `MessageQueue` for outgoing messages
  - an `FLRunMeta` for the run's meta information, filled with the public keys of joining clients
  - a `state` attribute used for lifecycle management of the whole run
- creates the `flRunOrch` instance with the `FLRunHandle`
- creates the `RelayClient`, which generates a private key and a certificate signing request (CSR) with the client
  ID as common name. The CSR is returned in the response of `/start-learning`.
- for app v1: creates the `LearningApiCommunicator` and the `AppCommunicatorV1` with a callback to it
- for app v2: adds the `FLRunHandle` to the `AppCommunicatorV2`

The run is not connected to the relay server yet. The caller has the CSR signed by the relay server
(`/sign-fl-run-cert`) and posts the certificate to `/start-relaying`. Only then the `RelayClient` connects, tying
the `FLRunHandle` state to its connection to the relay server, and the request returns once the connection is
established. Apps may already send before that; these messages wait in the `MessageQueue`.

```text
global learning api -> relay       POST /create-fl-run     -> channel, client IDs and keys
local learning api  -> controller  POST /start-learning    -> {"csr": "<PEM>"}
global learning api -> relay       POST /sign-fl-run-cert  {channel, clientId, csr} -> {"certificate": "<PEM>"}
local learning api  -> controller  POST /start-relaying    {channel, appKey, certificate}
```

**Incoming messages:** the `RelayClient` loads the message into the `MessageStore` and sets the notify channel.

- App v1: this wakes up the run's `AppCommunicatorV1` watcher, which posts all stored messages to the app.
- App v2: the app requests specific messages from the `AppCommunicatorV2` HTTP server; no further handling is needed.

**Outgoing messages:** `AppCommunicatorV1` and `AppCommunicatorV2` enqueue the message and set the notify channel.
This wakes up the `RelayClient`, which end-to-end encrypts and sends the message.

##### Errors

If a component runs into a critical issue, e.g. the connection to the relay server breaks, it changes
`FLRunHandle.state`. This cascades to all other components, which then fail when appropriate.

##### Stopping a federated learning run

The `FLRunManagerService` receives a stop request and the `FLRunManagerBO` marks the `FLRunHandle` as finished.
This clears the `MessageQueue` and `MessageStore` and reports the number of discarded messages. The `flRunOrch` is
removed and all remaining goroutines stop as they react to the finished state.

## Acknowledgements

This project is based on the [FeatureCloud](https://featurecloud.ai/) controller and relay server.

Matschinske, J., Späth, J., Bakhtiari, M., Probul, N., Kazemi Majdabadi, M. M., Nasirigerdeh, R., ... & Baumbach, J.
(2023). The FeatureCloud platform for federated learning in biomedicine: unified approach. Journal of Medical
Internet Research, 25, e42621.

Contributors are listed in [contributions.md](contributions.md). 
Please note that this does not include the full list of contributors to the FeatureCloud controller/relay server 
this is based on, but any changes done for FL-Net.
