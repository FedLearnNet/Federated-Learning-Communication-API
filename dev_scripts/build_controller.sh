#!/bin/sh
# Run from project root regardless of where the script is invoked from
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
set -e

docker build --tag feddb_fc_controller .
