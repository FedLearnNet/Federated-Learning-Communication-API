# Purpose
This repository contains two services that can be used to created federated learning 
networks and run federated learnings.

# General description
Generally this repo contains two different services.
- Controller
- Relay Server

Each controller is a federated learning node all connected via the one relay server to conduct
federated learning.

# Features
- All communication between controllers is end to end encrypted except the broadcasting of data from the aggregator
- Both SMPC and DP are supported
- Clients may drop out without the federated learning process stopping if the app is configured for this
  - SMPC is an exception, as this creates shards that get sent from one client to ALL other clients. All shards are required to reconstruct the data.
- The controller clients authenticate towards the relay server and the relay server authenticates
towards the clients.

# Deployment
You can either deploy via docker or build and deploy native.

## Relay Server
To deploy via docker use `Dockerfile.relay`. 

For native building use `go build -o relay ./cmd/relay`.
The relay server starts an http server at port `9140` and a tcp server at port `9141`. 

### Relay Configuration (CLI Flags & Environment Variables)
All configuration can be set via CLI flags, environment variables, or a `config.yml` file.
See `cmd/relay/main.go` for the complete list of flags. Key configuration options include:

- **Mode**: `--mode` or env `RELAY_MODE` (default: `prod`) — Set to `dev` for development
- **TLS Mode**: `--tls-mode` or env `RELAY_TLS_MODE` (default: `on`) — Options: `off`, `self-signed`, `on`
  - `prod` mode enforces `on` only (validates certificate chains)
  - `dev` mode allows `self-signed` for testing without valid certs
- **Domain**: `--domain` or env `RELAY_DOMAIN` (required for TLS) — e.g. `localhost`, `example.com`
- **HTTP Port**: `--http-port` or env `RELAY_HTTP_PORT` (default: `9140`)
- **TCP Port**: `--tcp-port` or env `RELAY_TCP_PORT` (default: `9141`)
- **Log Levels**: `--logLevelStdOut` and `--logLevelFile` (default: `info` and `debug`)

**Example (development with self-signed certs):**
```bash
./relay --mode dev --tls-mode self-signed --domain localhost
```

All prebuilt images via the CICD uses prod settings, so for development you most likely have to override the `RELAY_MODE` and `RELAY_TLS_MODE` environment variables.

> Note: when a developer connects through Docker host networking or a bridge host alias such as `docker.host.internal`, the self-signed certificate must be issued for that exact hostname. Example: `--domain docker.host.internal` and the controller must connect to `docker.host.internal:9141`.

**Example (CLI flags + Docker):**
```bash
docker run -e RELAY_MODE=dev -e RELAY_TLS_MODE=self-signed -e RELAY_DOMAIN=localhost fc_relay_e2etest
```

## Controller
To deploy via docker use `Dockerfile`, `Dockerfile.dev` or `Dockerfile.staging`.
They only differ in the `config.yaml` file used by them.

For native building use `go build -o controller ./cmd/controller`.

### Controller Configuration (CLI Flags & Environment Variables)
All configuration can be set via 
- CLI flags
- environment variables
- a `config.yml` file
with the list order also being the priority order.
If neither of these is given, the default values in `./cmd/controller/main.go` are used.
See `cmd/controller/main.go` for the complete list of flags. Key configuration options include:

- **Relay Address**: `--address-tcp` or env `RELAY_ADDRESS_TCP` (default: `localhost:9150`) — Where to connect to the relay server
- **Relay Mode**: `--mode` or env `RELAY_MODE` (default: `prod`) — Must match the relay server mode (`dev` or `prod`)
- **Relay TLS Mode**: `--tls-mode` or env `RELAY_TLS_MODE` (default: `on`) — Must match the relay server (`off`, `self-signed`, `on`)
- **HTTP Port**: `--flrunmanagerport` or env `FL_RUN_MANAGER_PORT` (default: `8080`)
- **AppCommV2 Port**: `--appcommv2port` or env `APPCOMMV2_PORT` (default: `8081`)
- **Log Levels**: `--logLevelStdOut` and `--logLevelFile` (default: `info` and `debug`)
- **Learning API**: `--learning-api-ws` or env `WORKFLOW_LEARNING_API_WS_ADDRESS` (default: `ws://localhost:8080/ws`)

**Example (development with self-signed certs, connecting to relay on port 9141):**
```bash
./controller --address-tcp localhost:9141 --mode dev --tls-mode self-signed
```

**Example (CLI flags + Docker):**
```bash
docker run -e RELAY_ADDRESS_TCP=relay:9141 -e RELAY_MODE=dev -e RELAY_TLS_MODE=self-signed \
  -e FL_RUN_MANAGER_PORT=8080 -e APPCOMMV2_PORT=8081 gitlab.cosy.bio:5050/cosybio/federated-learning/federated_db/feature-cloud-controller/controller:staging
```

The default settings of the prebuild images depend on the image:
- Dev image built using `Dockerfile.dev` uses `config.yaml`
- Staging image built using `Dockerfile.staging` uses `config.staging.yml` (also built in the CICD pipeline)
- Prod image built using `Dockerfile` uses `config.prod.yml` (also built in the CICD pipeline)

### Service Connectivity
The controller service connects to:
- a relay server (configured via `--address-tcp`)
- when starting an app version 1: a local learning api (websocket)
  - not yet implemented

# Architecture
## Package: Shared
A shared package containing helper code that is used by both the Controller and the Relay Server.
This contains e.g. read/write helpers around data sent between the Controller and Relay server
or logging functionalities:
- `pkg/shared/logger`: `Logger`
- `pkg/shared/link`: Contains read and write helpers for messages between controller and relay server.
- `pkg/shared/models`: Contains common models used by both, e.g. a clientID
- `pkg/shared/util`: Contains common utilities such as e.g. crypto helpers.

## Package: Relay Server
### Purpose
The relay server creates a specific federated learning run, including relevant
authentication keys, lets clients (controller instances) connect and relays information.
It also keeps track of relevant meta information of the runs, e.g. the public keys of clients.
The relay server does NOT manage the orchestration of federated learning runs and this must be
done via the HTTP server the relay server exposes.

### Architecture
The relay server contains the following components:
- `pkg/relayserver/http`: `RelayServiceHTTP`: a HTTP server that can be used to create and stop federated learning runs
- `pkg/relayserver/tcp`: `RelayServiceTCP`: a TCP server that the controllers connect to to relay information via this service.
- `pkg/relayserver/bridge`: `FLRunStore`: a map containing the meta information of the ongoing federated learning runs.
Managed by `RelayServiceHTTP` while `RelayServiceTCP` uses it. Each run has an atomic `State` field
for lifecycle coordination between HTTP and TCP components.

The federated learning runs created at `RelayServiceHTTP` are performed via `RelayServiceTCP`.

The entrypoint to the relay server is at `cmd/relay/main.go`

### Flow
The entrypoint in `main.go` creates the `FlRunStore` and starts both `RelayServiceHTTP` and `RelayServiceTCP`.
When a run is created, it's added to the store and `RelayServiceTCP` can access it when clients connect.
When `RelayServiceHTTP` stops a run, it marks the run's `State` as finished. The TCP relay goroutines
detect this state change via `IsActive()` and exit cleanly, allowing graceful shutdown of all
connections and cleanup of any pending messages.

## Package: Controller
### Purpose
Each controller represents one federated learning node, controlling the flow there.
The controller is designed to:
- Relay information from a federated learning app in a federated learning network formed by
multiple controllers and one relay server
- Apply privacy enhancing techniques such as SMPC and DP as well as end to end encryption for
peer to peer sending in the federated learning process

The controller does NOT manage the orchestration of apps and needs an external service to
start and stop federated learnings.

### Architecture
The controller contains multiple components with subcomponents:
- `pkg/controller`: `FLRunManagerServiceHTTP`: HTTP server running on the `flrunmanagerport`, receives orchestration requests
- `pkg/controller`: `FLRunManagerBO`: manages the lifecycle of individual FL runs, owns multiple `flRunOrch` instances:
  - `map[FlRunKey]flRunOrch`:
    - `pkg/controller`: `flRunOrch`:
      - `pkg/controller`: `FLRunHandle`: a shared handle containing the communication bridge for a run:
        - `pkg/controller/bridge`: `MessageStore` / `MessageQueue` / `FLRunMeta` (one per run)
      - `pkg/controller/relaycomm`: `RelayClient`
      - `pkg/controller/appcomm`: `AppCommunicatorV1` (one per run, V1 only)
      - `pkg/controller/learningcomm`: `LearningApiCommunicator` (websocket client, one per run V1 only)
  - `pkg/controller/appcomm`: `AppCommunicatorV2`: HTTP server running on the `appcommv2port`,
  singleton, used for multiple concurrent runs. Holds a `map[FlRunKey]*FLRunHandle`, with each
  entry pointing to the same `FLRunHandle` owned by the corresponding `flRunOrch`.
Furthermore, there are some helper components:
- `pkg/controller/data`: Contains HTTP models, enums, etc.

The entrypoint to the controller is at `cmd/controller/main.go`

### Flow
#### Start of a federated learning
On controller startup, the `FLRunManagerService` of the flow package is started.
Also starts the `AppCommunicatorV2`, as the same instance is used for multiple concurrent runs.

The complete flow of one federated learning run is then the following:

First, the `FLRunManagerService` receives a request to start a federated learning run, invoking the
relevant method of the `FLRunManagerBO`. This:
- creates an `FLRunHandle` which contains
  - a `MessageStore` for incoming messages
  - a `MessageQueue` for outgoing messages
  - a `FLRunMeta` for the run's meta information, slowly filled with the public keys of other joining clients
  - a `state` attribute used for lifecycle management of the whole run by subsequent components.
- creates the `flRunOrch` instance with the `FLRunHandle`
- creates the `RelayClient` instance with the `FLRunHandle`
- starts the `RelayClient`, which connects the `state` from the  `FLRunHandle` with it's connection to the global relay server
- if app v1: 
  - creates the `LearningApiCommunicator` with the `FLRunHandle` 
  - creates the `AppCommunicatorV1` with the `FLRunHandle`  and a callback to the `LearningApiCommunicator`
- if app v2:
  - the `AppCommunicatorV2` is invoked with the method to add the `FLRunHandle` to it

The exact handling of messages depends on the app version:

**Incoming messages**
The `RelayClient` loads the message into the `MessageStore` and sets the relevant notify channel.

*App V1*
This wakes up the FL run's `AppCommunicatorV1` incoming message watcher, which requests all
messages currently in the store and posts them to the app.

*App V2*
As the app requests a specific message from the HTTP server provided by the `AppCommunicatorV2`,
nothing else is done. The notify channel is not required here.

**Outgoing messages**
Both `AppCommunicatorV1` and `AppCommunicatorV2` enqueue the message and set the relevant
notify channel. This wakes up the `RelayClient` outgoing message watcher, which E2E encrypts
and sends the message.

#### Error of the federated learning
If any service run into a critical issue, e.g. the connection to the global relay server breaks,
this changes the `FLRunHandle.state`. This cascades to all other components who then fail when 
appropriate.

#### Stopping of a federated learning
The `FLRunManagerService` receives a request to stop this federated learning run. This invokes
the `FLRunManagerBO` cleanup method, which simply marks the `FLRunHandle` as finished.
This mark clears the `MessageQueue` and `MessageStore`, reporting the amount of discarded messages.
The `flRunOrch` is removed and all goroutines still running gracefully stop as they react on
the `FLRunHandle.state` being finished.

# End-to-end tests
Integration tests for plain, DP, and SMPC message flows live in `e2etests/`.
Run from the project root with `bash e2etests/e2e_test.sh`. Requires Python 3 with
`pip install -r e2etests/requirements.txt`.

# Acknowledgements
This project was developed based on the [FeatureCloud](https://featurecloud.ai/) Controller and Relay Server. 
Reference:
Matschinske, J., Späth, J., Bakhtiari, M., Probul, N., Kazemi Majdabadi, M. M., Nasirigerdeh, R., ... & Baumbach, J. (2023). The FeatureCloud platform for federated learning in biomedicine: unified approach. Journal of Medical Internet Research, 25, e42621.
