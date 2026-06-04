#!/bin/zsh

set -euo pipefail

if [[ "${OSTYPE:-}" != darwin* ]]; then
  echo "Bu script yalnizca macOS icin tasarlandi."
  exit 1
fi

KEYCHAIN_ACCOUNT="default"
KEYCHAIN_PREFIX="MustafaWebProje"

restore_tty() {
  stty echo 2>/dev/null || true
}

trap restore_tty EXIT INT TERM

save_secret() {
  local secret_name="$1"
  local secret_value="$2"

  /usr/bin/security add-generic-password \
    -U \
    -a "$KEYCHAIN_ACCOUNT" \
    -s "${KEYCHAIN_PREFIX}/${secret_name}" \
    -w "$secret_value" >/dev/null
}

read_hidden_optional() {
  local prompt="$1"
  local value=""

  printf "%s" "$prompt" >&2
  stty -echo
  read -r value
  restore_tty
  printf "\n" >&2

  printf "%s" "$value"
}

printf "OBUS kullanici adini girin [busproductapp]: "
read -r obus_username
obus_username="${obus_username:-busproductapp}"

printf "OBUS sifresini girin: "
stty -echo
read -r obus_password
restore_tty
printf "\n"

if [[ -z "$obus_username" || -z "$obus_password" ]]; then
  echo "Kullanici adi ve sifre bos birakilamaz."
  exit 1
fi

save_secret "OBUS_SERVICE_LOGIN_USERNAME" "$obus_username"
save_secret "OBUS_SERVICE_LOGIN_PASSWORD" "$obus_password"
save_secret "INVENTORY_BRANCHES_LOGIN_USERNAME" "$obus_username"
save_secret "INVENTORY_BRANCHES_LOGIN_PASSWORD" "$obus_password"
save_secret "OBUS_JOB_FIXED_USERNAME" "$obus_username"
save_secret "OBUS_JOB_FIXED_PASSWORD" "$obus_password"
save_secret "OBUS_USER_CREATE_LOGIN_USERNAME" "$obus_username"
save_secret "OBUS_USER_CREATE_LOGIN_PASSWORD" "$obus_password"

saved_count=8

sql_password="$(read_hidden_optional "Obus user deactivate SQL sifresini girin [bos birak=atla]: ")"
if [[ -n "$sql_password" ]]; then
  save_secret "OBUS_USER_DEACTIVATE_SQL_PASSWORD" "$sql_password"
  saved_count=$((saved_count + 1))
fi

sql_proxy_token="$(read_hidden_optional "Obus user deactivate SQL proxy token girin [bos birak=atla]: ")"
if [[ -n "$sql_proxy_token" ]]; then
  save_secret "OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN" "$sql_proxy_token"
  saved_count=$((saved_count + 1))
fi

trap - EXIT INT TERM

echo "$saved_count secret macOS Keychain'e kaydedildi."
echo "Uygulamayi yeniden baslatin."
