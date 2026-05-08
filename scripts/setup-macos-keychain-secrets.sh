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

printf "OBUS kullanici adini girin: "
read -r obus_username

printf "OBUS sifresini girin: "
stty -echo
read -r obus_password
restore_tty
printf "\n"

if [[ -z "$obus_username" || -z "$obus_password" ]]; then
  echo "Kullanici adi ve sifre bos birakilamaz."
  exit 1
fi

save_secret "INVENTORY_BRANCHES_LOGIN_USERNAME" "$obus_username"
save_secret "INVENTORY_BRANCHES_LOGIN_PASSWORD" "$obus_password"
save_secret "OBUS_JOB_FIXED_USERNAME" "$obus_username"
save_secret "OBUS_JOB_FIXED_PASSWORD" "$obus_password"
save_secret "OBUS_USER_CREATE_LOGIN_USERNAME" "$obus_username"
save_secret "OBUS_USER_CREATE_LOGIN_PASSWORD" "$obus_password"

trap - EXIT INT TERM

echo "6 secret macOS Keychain'e kaydedildi."
echo "Uygulamayi yeniden baslatin."
