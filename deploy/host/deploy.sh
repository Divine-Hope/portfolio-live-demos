#!/usr/bin/env bash
# Called by the deploy workflow through SSM Run Command, as root:
#   /opt/livedemos/deploy/host/deploy.sh <commit sha>
# Checks out that commit, refreshes secrets, pulls the image built from it, and
# recreates whatever changed. The workflow records the tag in SSM before calling this.
set -euo pipefail

main() {
  local sha=${1:-}
  if [[ ! $sha =~ ^[0-9a-f]{40}$ ]]; then
    echo "usage: deploy.sh <full commit sha>" >&2
    return 2
  fi
  cd /opt/livedemos
  git fetch --quiet --depth 1 origin "$sha"
  git checkout --quiet --force "$sha"

  deploy/host/render-env.sh
  if ! grep -q "^IMAGE_TAG=$sha\$" .env; then
    echo "SSM image-tag doesn't match $sha; refusing to deploy" >&2
    return 3
  fi

  local compose=(docker compose -f compose.yaml -f compose.prod.yaml)
  "${compose[@]}" pull --quiet
  "${compose[@]}" up -d --remove-orphans
  docker image prune --force --filter "until=168h" >/dev/null
  echo "deployed $sha"
}

# The checkout above rewrites this file while it runs. Bash has read all of main()
# by the time it's called, and exits straight after, so the new version can't interfere.
main "$@"
exit $?
