#!/usr/bin/env bash
# Keep a live nora OIDC token in a file for the image build (make docker-build / docker-buildx). The token lives minutes, and BuildKit
# reads a secret FILE each time a RUN mounts it, so a RUN late in a long build (arm64 under
# QEMU) still gets a live one. Same loop as github-actions dotnet-service-build.yml.
# Needs `id-token: write`. Exports PRIVATE_PACKAGES_TOKEN_FILE to the job.
set -euo pipefail
dir="$RUNNER_TEMP/packages-token"; mkdir -p "$dir"; chmod 700 "$dir"
file="$dir/PRIVATE_PACKAGES_TOKEN"
write() { printf '%s' "$1" > "$dir/.t" && mv -f "$dir/.t" "$file"; } # atomic for a reader
mint() {
  curl -sSf --retry 3 -H "Authorization: bearer ${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" \
    "${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=packages.maestra.io" | jq -r '.value // empty'
}
t=$(mint) && [ -n "$t" ] || { echo "::error::cannot mint the packages OIDC token"; exit 1; }
echo "::add-mask::$t"
write "$t"
echo "PRIVATE_PACKAGES_TOKEN_FILE=$file" >> "$GITHUB_ENV"
# Lives until the runner's orphan cleanup at the end of the job.
( while sleep 60; do t=$(mint) && [ -n "$t" ] && write "$t"; done ) </dev/null >/dev/null 2>&1 &
