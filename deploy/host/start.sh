#!/usr/bin/env bash
# Runs at boot (livedemos.service): refresh secrets, then bring the stack up with the
# image tag currently recorded in SSM. Before the first deploy there's nothing to run.
set -euo pipefail
cd /opt/livedemos

deploy/host/render-env.sh
if ! grep -Eq '^IMAGE_TAG=[0-9a-f]{40}$' .env; then
  echo "no image deployed yet; the first deploy starts the stack"
  exit 0
fi
docker compose -f compose.yaml -f compose.prod.yaml up -d --remove-orphans
