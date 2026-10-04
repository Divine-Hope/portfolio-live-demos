#!/usr/bin/env bash
# Write /opt/livedemos/.env from SSM Parameter Store: root-only, rewritten every time.
#
# Every parameter under the prefix becomes one variable, named after it:
#   /livedemos/clickhouse-api-password  ->  CLICKHOUSE_API_PASSWORD
# so adding a setting is a Terraform change, never a change here.
set -euo pipefail

# shellcheck source=/dev/null
source /etc/livedemos/host.env   # AWS_REGION and SSM_PREFIX, written by the user data

app_dir=/opt/livedemos
umask 077
tmp=$(mktemp "$app_dir/.env.XXXXXX")
trap 'rm -f "$tmp"' EXIT

aws ssm get-parameters-by-path \
  --region "$AWS_REGION" --path "$SSM_PREFIX" --with-decryption \
  --query 'Parameters[].[Name,Value]' --output text |
  while IFS=$'\t' read -r name value; do
    key=${name#"$SSM_PREFIX"/}
    key=${key//-/_}
    printf '%s=%s\n' "${key^^}" "$value"
  done >"$tmp"

chmod 600 "$tmp"
mv "$tmp" "$app_dir/.env"
trap - EXIT
echo "wrote $app_dir/.env ($(wc -l <"$app_dir/.env") settings)"
