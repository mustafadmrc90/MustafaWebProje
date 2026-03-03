#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Kullanim:
  ./scripts/init-db-only.sh [DATABASE_URL]

Ornek:
  ./scripts/init-db-only.sh
  ./scripts/init-db-only.sh "Server=18.159.75.68,1433;Database=obilet-b2b-preprod;User Id=corp_mdemirci;Password=***;Encrypt=True;TrustServerCertificate=True;"

Bu komut:
  - Uygulamadaki initDb + sidebar sync islemlerini calistirir
  - Sunucuyu acmadan cikar
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

DATABASE_URL_INPUT="${1:-${DATABASE_URL:-}}"
if [[ -z "$DATABASE_URL_INPUT" && -f ".env" ]]; then
  DATABASE_URL_INPUT="$(
    awk -F= '
      /^[[:space:]]*DATABASE_URL[[:space:]]*=/ {
        line = substr($0, index($0, "=") + 1);
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", line);
        print line;
        exit;
      }
    ' .env
  )"
fi

if [[ -z "$DATABASE_URL_INPUT" ]]; then
  echo "DATABASE_URL bulunamadi. Arguman verin veya ortam degiskeni set edin."
  exit 1
fi

DATABASE_URL="$DATABASE_URL_INPUT" \
NODE_ENV=production \
INIT_DB_ONLY=true \
node server.js
