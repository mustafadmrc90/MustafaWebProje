#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Kullanim:
  ./scripts/migrate-postgres.sh "<SOURCE_DATABASE_URL>" "<TARGET_DATABASE_URL>" [OUTPUT_DIR]

Ornek:
  ./scripts/migrate-postgres.sh \
    "postgresql://source_user:pass@source-host:5432/source_db?sslmode=require" \
    "postgresql://target_user:pass@target-host:5432/postgres?sslmode=require"

Notlar:
  - Komut kaynak DB'den plain SQL dump alir ve hedef DB'ye yukler.
  - Varsayilan olarak cikti data/migrations/<timestamp>/ altina yazilir.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage
  exit 1
fi

SOURCE_DATABASE_URL="$1"
TARGET_DATABASE_URL="$2"
OUTPUT_DIR="${3:-data/migrations/$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$OUTPUT_DIR"
SQL_DUMP_FILE="$OUTPUT_DIR/source.sql"

echo "[1/5] Kaynak DB baglantisi test ediliyor..."
psql "$SOURCE_DATABASE_URL" -v ON_ERROR_STOP=1 -c "select now();" >/dev/null

echo "[2/5] Hedef DB baglantisi test ediliyor..."
psql "$TARGET_DATABASE_URL" -v ON_ERROR_STOP=1 -c "select now();" >/dev/null

echo "[3/5] Kaynak DB dump aliniyor: $SQL_DUMP_FILE"
pg_dump \
  "$SOURCE_DATABASE_URL" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  --format=plain \
  --file="$SQL_DUMP_FILE"

echo "[4/5] Dump hedef DB'ye yukleniyor..."
psql "$TARGET_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$SQL_DUMP_FILE" >/dev/null

echo "[5/5] Tamamlandi."
echo "Dump dosyasi: $SQL_DUMP_FILE"
