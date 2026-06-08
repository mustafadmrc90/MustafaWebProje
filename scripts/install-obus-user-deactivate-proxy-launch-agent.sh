#!/bin/zsh

set -euo pipefail

if [[ "${OSTYPE:-}" != darwin* ]]; then
  echo "Bu script yalnizca macOS icin tasarlandi."
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PLIST_DIR="$HOME/Library/LaunchAgents"
LOG_DIR="$HOME/Library/Logs/MustafaWebProje"
PLIST_PATH="$PLIST_DIR/com.mustafawebproje.obus-user-deactivate-proxy.plist"
NODE_PATH="$(command -v node || true)"
NPM_PATH="$(command -v npm || true)"
USER_ID="$(id -u)"

if [[ -z "$NODE_PATH" || -z "$NPM_PATH" ]]; then
  echo "node veya npm bulunamadi. Once Node.js/npm kurulumunu kontrol edin."
  exit 1
fi

mkdir -p "$PLIST_DIR" "$LOG_DIR"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.mustafawebproje.obus-user-deactivate-proxy</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>cd "$PROJECT_DIR" &amp;&amp; "$NPM_PATH" run obus-user-deactivate-sql-proxy</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$PROJECT_DIR</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$LOG_DIR/obus-user-deactivate-proxy.out.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/obus-user-deactivate-proxy.err.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>$(dirname "$NODE_PATH"):$(dirname "$NPM_PATH"):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
EOF

launchctl bootout "gui/$USER_ID" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$USER_ID" "$PLIST_PATH"
launchctl kickstart -k "gui/$USER_ID/com.mustafawebproje.obus-user-deactivate-proxy" >/dev/null 2>&1 || true

echo "LaunchAgent kuruldu: $PLIST_PATH"
echo "Loglar:"
echo "  $LOG_DIR/obus-user-deactivate-proxy.out.log"
echo "  $LOG_DIR/obus-user-deactivate-proxy.err.log"
echo "Kontrol:"
echo "  curl http://127.0.0.1:3015/health"
