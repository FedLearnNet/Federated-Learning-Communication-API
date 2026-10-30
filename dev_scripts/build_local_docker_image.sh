#!/bin/sh
# Run from project root regardless of where the script is invoked from
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
set -e

docker build . -t controller:testingtag
docker build . --file Dockerfile.relay -t controller-relay:testingtag

