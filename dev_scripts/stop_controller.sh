#!/bin/sh
# Run from project root regardless of where the script is invoked from
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

ids=$(docker ps -a -q --filter "label=fc-controller-label")
[ -n "$ids" ] && docker rm -f $ids

docker kill fc-controller > /dev/null 2>&1
docker rm fc-controller > /dev/null 2>&1
docker network rm fc-controller-internal > /dev/null 2>&1

docker volume prune --force --filter "label=fc-controller-label"
docker network prune --force --filter "label=fc-controller-label"
