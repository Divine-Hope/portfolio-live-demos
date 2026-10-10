#!/usr/bin/env bash
# Markdown for the job summary: the counts, then each changed resource's address and
# actions. Never the plan text: the repo is public, and attribute values carry the account
# ID (bucket names, ARNs) and the budget email.
set -euo pipefail

plan=$1
changes=$(terraform show -json "$plan" |
  jq -c '[.resource_changes[]? | select(.change.actions != ["no-op"] and .change.actions != ["read"])
          | {address, actions: (.change.actions | join("/"))}] | sort_by(.address)')
counts=$(jq -r 'group_by(.actions) | map("\(.[0].actions): \(length)")
  | if length == 0 then "no changes" else join(", ") end' <<<"$changes")

echo "### terraform plan: $counts"
if [ "$changes" != "[]" ]; then
  echo
  echo '| Action | Resource |'
  echo '|---|---|'
  jq -r '.[] | "| \(.actions) | `\(.address)` |"' <<<"$changes"
fi
