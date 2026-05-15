#!/usr/bin/env zsh
set -e

COMMIT_HASH=$(git rev-parse --short HEAD)
PATCH_FILE="${COMMIT_HASH}.patch"

git diff HEAD > "$PATCH_FILE"
echo "Patch generated: $PATCH_FILE"