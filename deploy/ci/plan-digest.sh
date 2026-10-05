#!/usr/bin/env bash
# What a saved plan would change, as GitHub step outputs:
#   changes=true|false
#   digest=<sha256 of every resource address and action, values left out>
# Fails if the plan deletes or replaces anything: those are done by hand
# (`make tf-plan tf-apply`), never by a merge.
set -euo pipefail

plan=$1
changes=$(terraform show -json "$plan" |
  jq -c '[.resource_changes[]? | select(.change.actions != ["no-op"] and .change.actions != ["read"])
          | {address, actions: .change.actions}] | sort_by(.address)')

destructive=$(jq -r '.[] | select(.actions | index("delete")) | .address' <<<"$changes")
if [ -n "$destructive" ]; then
  echo "::error::this plan deletes or replaces resources; apply it by hand with make tf-plan tf-apply:" >&2
  echo "$destructive" >&2
  exit 1
fi

if [ "$changes" = "[]" ]; then echo "changes=false"; else echo "changes=true"; fi
echo "digest=$(sha256sum <<<"$changes" | cut -d' ' -f1)"
