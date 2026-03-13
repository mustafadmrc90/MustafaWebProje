#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Kullanim:
  ./scripts/find-supabase-connection.sh <project_ref> <db_password>

Ornek:
  ./scripts/find-supabase-connection.sh agcgnagytkfobyddmiwy 'Mstfdmrc123..'

Aciklama:
  - Supabase pooler baglanti formatlarini otomatik dener.
  - Ilk calisan URL'yi yazdirir.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 2 ]]; then
  usage
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql bulunamadi."
  exit 1
fi

PROJECT_REF="$(echo "$1" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9')"
RAW_PASSWORD="$2"
ENCODED_PASSWORD="$(node -e 'console.log(encodeURIComponent(process.argv[1] || ""))' "$RAW_PASSWORD")"

if [[ -z "$PROJECT_REF" ]]; then
  echo "Gecersiz project_ref."
  exit 1
fi

REGIONS=(
  ap-southeast-2
  ap-southeast-1
  ap-northeast-1
  ap-northeast-2
  ap-south-1
  eu-central-1
  eu-west-1
  eu-west-2
  us-east-1
  us-west-1
  us-west-2
  ca-central-1
  sa-east-1
  me-central-1
)

USER_CANDIDATES=("postgres.${PROJECT_REF}" "postgres")
POOLER_PREFIXES=("aws-0" "aws-1")
PORTS=(5432 6543)

try_url() {
  local url="$1"
  if PGCONNECT_TIMEOUT=6 psql "$url" -v ON_ERROR_STOP=1 -c "select now();" >/dev/null 2>&1; then
    echo "$url"
    return 0
  fi
  return 1
}

for user in "${USER_CANDIDATES[@]}"; do
  for prefix in "${POOLER_PREFIXES[@]}"; do
    for region in "${REGIONS[@]}"; do
      for port in "${PORTS[@]}"; do
        URL="postgresql://${user}:${ENCODED_PASSWORD}@${prefix}-${region}.pooler.supabase.com:${port}/postgres?sslmode=require"
        if WORKING_URL="$(try_url "$URL")"; then
          echo "BULUNDU:"
          echo "$WORKING_URL"
          exit 0
        fi
      done
    done
  done
done

echo "Calisan pooler URL bulunamadi."
echo "Supabase panelde Connect altinda verilen URI'yi manuel kopyalayin."
exit 1
