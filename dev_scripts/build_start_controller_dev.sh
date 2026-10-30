#!/bin/sh
# Run from project root regardless of where the script is invoked from
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

docker build -t feddb_fc_controller -f ./Dockerfile.dev .
mkdir -p data

docker kill fc-controller > /dev/null 2>&1 
docker rm fc-controller > /dev/null 2>&1

docker run \
 -p 8002:8002 \
 --name fc-controller \
 -v "/var/run/docker.sock:/var/run/docker.sock" \
 --mount "type=bind,source=$(pwd)/data,target=/data" \
 feddb_fc_controller "--internal-root=/data"
