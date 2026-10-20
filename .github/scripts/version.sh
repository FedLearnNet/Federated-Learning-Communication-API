#!/usr/bin/env bash
# Knows where this repository keeps its version, and nothing more.
# The rules (format, which branch takes which version, what gets released) are the same for all FL-Net
# repositories, so they live once in FedLearnNet/.github (actions/version) instead of in every copy of this file.
# See "Release workflow" in the contribution guide of the Documentation.
#
# Usage:
#   version.sh                  print the version
#   version.sh set <version>    change the version, e.g. 1.1.0.dev, 1.1.0.rc0 or 1.1.0
set -euo pipefail
cd "$(dirname "$0")/../.."

# Stored in VERSION
read_version() { tr -d '[:space:]' < VERSION; echo; }
write_version() { echo "$1" > VERSION; }

case "${1:-}" in
  "")
    read_version
    ;;
  set)
    write_version "${2:?Usage: version.sh set <version>}"
    read_version
    ;;
  *)
    echo "Usage: version.sh [set <version>]" >&2
    exit 2
    ;;
esac
