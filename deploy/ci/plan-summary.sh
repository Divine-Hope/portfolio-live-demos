#!/usr/bin/env bash
# Markdown for the job summary: the one-line count, then the full plan (sensitive values
# are already redacted by Terraform).
set -euo pipefail

plan=$1
text=$2
counts=$(terraform show -json "$plan" | jq -r '
  [.resource_changes[]?.change.actions | join("/")] | group_by(.) | map("\(.[0]): \(length)")
  | map(select(startswith("no-op") | not)) | if length == 0 then "no changes" else join(", ") end')

echo "### terraform plan: $counts"
echo
echo '<details><summary>Full plan</summary>'
echo
echo '```'
cat "$text"
echo '```'
echo '</details>'
