#!/bin/zsh

set -euo pipefail

if [[ "${OSTYPE:-}" != darwin* ]]; then
  echo "Bu script yalnizca macOS icin tasarlandi."
  exit 1
fi

PLIST_PATH="$HOME/Library/LaunchAgents/com.mustafawebproje.obus-user-deactivate-proxy.plist"
USER_ID="$(id -u)"

launchctl bootout "gui/$USER_ID" "$PLIST_PATH" >/dev/null 2>&1 || true
rm -f "$PLIST_PATH"

echo "Obus user deactivate proxy LaunchAgent kaldirildi."
