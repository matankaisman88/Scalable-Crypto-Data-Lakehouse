#!/bin/bash
# Cleanup helper: remove raw data folders that don't match the desired resolution.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

RESOLUTION_ARG="$1"
RESOLUTION="${RESOLUTION_ARG:-${RESOLUTION:-1m}}"

echo "Cleaning data/raw to keep only resolution: ${RESOLUTION}"
echo "Base directory: data/raw"

# Assumes layout: data/raw/<SYMBOL>/<RESOLUTION>/
# Removes any resolution-level directory that does not match ${RESOLUTION}
if [ -d "data/raw" ]; then
  # Use a portable pattern: list first, then delete via xargs
  echo "Will remove the following directories (if any):"
  find data/raw -mindepth 2 -maxdepth 2 -type d ! -name "${RESOLUTION}"

  find data/raw -mindepth 2 -maxdepth 2 -type d ! -name "${RESOLUTION}" -print0 | xargs -0 -r rm -rf
else
  echo "Directory data/raw does not exist; nothing to clean."
fi

